"""Minimal read-only S3 ListObjectsV2 client (AWS Signature V4)."""
import datetime
import hashlib
import hmac
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


def list_objects(prefix):
    token = None
    objects = []
    while True:
        args = {'list-type':'2', 'prefix':prefix}
        if token:
            args['continuation-token'] = token
        query = '&'.join(urllib.parse.quote(k,safe='')+'='+urllib.parse.quote(v,safe='') for k,v in sorted(args.items()))
        now = datetime.datetime.now(datetime.timezone.utc)
        stamp, day = now.strftime('%Y%m%dT%H%M%SZ'), now.strftime('%Y%m%d')
        empty = hashlib.sha256(b'').hexdigest()
        headers = {'host':'minio:9000','x-amz-content-sha256':empty,'x-amz-date':stamp}
        names = ';'.join(sorted(headers))
        canonical_headers = ''.join(k+':'+headers[k]+'\n' for k in sorted(headers))
        canonical = '\n'.join(['GET','/warehouse',query,canonical_headers,names,empty])
        scope = day+'/us-east-1/s3/aws4_request'
        to_sign = 'AWS4-HMAC-SHA256\n'+stamp+'\n'+scope+'\n'+hashlib.sha256(canonical.encode()).hexdigest()
        key = b'AWS4minioadmin'
        for part in (day,'us-east-1','s3','aws4_request'):
            key = hmac.new(key,part.encode(),hashlib.sha256).digest()
        signature = hmac.new(key,to_sign.encode(),hashlib.sha256).hexdigest()
        headers['Authorization'] = f'AWS4-HMAC-SHA256 Credential=minioadmin/{scope}, SignedHeaders={names}, Signature={signature}'
        req = urllib.request.Request('http://minio:9000/warehouse?'+query,headers=headers)
        with urllib.request.urlopen(req,timeout=120) as response:
            xml = ET.fromstring(response.read())
        ns = {'s':'http://s3.amazonaws.com/doc/2006-03-01/'}
        for obj in xml.findall('s:Contents',ns):
            objects.append({'key':obj.findtext('s:Key',namespaces=ns),'bytes':int(obj.findtext('s:Size',namespaces=ns))})
        token = xml.findtext('s:NextContinuationToken',namespaces=ns)
        if not token:
            return objects


def storage_metrics(file_paths):
    roots = set()
    for (path,) in file_paths:
        key = urllib.parse.urlparse(path).path.lstrip('/')
        if '/data/' not in key:
            raise ValueError('Unexpected Iceberg file location: '+path)
        roots.add(key.split('/data/',1)[0]+'/')
    objects = [obj for prefix in roots for obj in list_objects(prefix)]
    return {'metadata_bytes':sum(x['bytes'] for x in objects if '/metadata/' in x['key']),
            'all_objects_bytes':sum(x['bytes'] for x in objects),'object_count':len(objects)}
