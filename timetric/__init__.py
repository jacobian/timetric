import csv
import dateutil.parser
import httplib2
import simplejson
import time
from cStringIO import StringIO
from oauth import oauth

SIGNATURE = oauth.OAuthSignatureMethod_HMAC_SHA1()

class TimetricClient(object):
    """
    Timetric client. You'll need a config dict; the authenticated token will be
    written back to this dictionary. Obviously you should make this persistant
    in some way to avoid needing to authenticate each time.
    """
    request_token_url = 'http://timetric.com/oauth/request_token/'
    authorization_url = 'http://timetric.com/oauth/authorize/'
    access_token_url = 'http://timetric.com/oauth/access_token/'
    
    def __init__(self, config):
        self.http = httplib2.Http()
        self.config = config
        try:
            self.consumer = oauth.OAuthConsumer(self.config['consumer_key'], self.config['consumer_secret'])
        except KeyError:
            raise KeyError('Config missing consumer key/secret.')
        try:
            self.access_token = oauth.OAuthToken(self.config['oauth_token'], self.config['oauth_secret'])
        except KeyError:
            self.access_token = None
            
    def series(self, id):
        """
        Get an existing data series. Fails if the client isn't successfully
        authorized.
        """
        if not self.access_token:
            raise ValueError("Client isn't yet authorized.")
        return Series(self, id)
        
    def create_series(self, **kw):
        """
        Create a new series.
        """
        raise NotImplementedError()
        
    def get_request_token(self):
        """
        Step 0: request an OAuth token. 
        
        Called automatically by get_authorize_url() if needed.
        """
        req = oauth.OAuthRequest.from_consumer_and_token(self.consumer, http_url=self.request_token_url)
        req.sign_request(SIGNATURE, self.consumer, None)
        resp, body = self.http.request(req.to_url(), 'GET')
        return oauth.OAuthToken.from_string(body)

    def get_authorize_url(self, token=None, callback=None):
        """
        Step 1: direct the user to the authentication URL.
        """
        if not token:
            token = self.get_request_token()
        req = oauth.OAuthRequest.from_token_and_callback(token=token, http_url=self.authorization_url, callback=callback)
        return req.to_url()
        
    def get_access_token(self, token):
        """
        Step 2: once authorized, convert the app token to an access token.
        
        Stores the access token in the config dict for later use.
        """
        req = oauth.OAuthRequest.from_consumer_and_token(self.consumer, token=token, http_url=self.access_token_url)
        req.sign_request(SIGNATURE, self.consumer, token)
        resp, body = self.http.request(req.to_url(), 'GET')
        self.access_token = oauth.OAuthToken.from_string(body)
        self.config['oauth_token'] = self.access_token.key
        self.config['oauth_secret'] = self.access_token.secret
        return self.access_token

    def request(self, url, method='GET', params={}, files={}):
        """
        Make an authenticated OAuth request.
        """
        if not self.access_token:
            raise ValueError("Client isn't yet authorized.")

        req = oauth.OAuthRequest.from_consumer_and_token(
            self.consumer,
            self.access_token,
            http_method = method,
            http_url = url,
            parameters = params
        )
        req.sign_request(SIGNATURE, self.consumer, self.access_token)        
                
        if method in ['GET', 'DELETE']:
            resp, body = self.http.request(req.to_url(), method)
        else:
            if files:
                body = _encode_multipart(params, files)
                headers = req.to_header()
                headers['Content-Type'] = MULTIPART_CONTENT
            else:
                body = req.to_postdata()
                headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            resp, body = self.http.request(
                uri = req.get_normalized_http_url(), 
                method = method, 
                body = body,
                headers = headers,
            )
        return body

class Series(object):
    """
    A Timetric data series.
    
    Don't create directly; use TimetricClient.series().
    """
    
    def __init__(self, client, id):
        self.client = client
        self.id = id
        self.url = 'http://timetric.com/series/%s/' % self.id
    
    def __repr__(self):
        return "<timetric.Series('%s')>" % self.id
    
    def latest(self):
        """
        Get the latest value in this series. Returns a tuple `(timestamp,
        value)`.
        """
        data = simplejson.loads(self.client.request(self.url + "value/json/", 'GET'))
        return (data['timestamp'], data['value'])
        
    def csv(self):
        """
        Get the raw CSV data (as a string) of this series.
        """
        return self.client.request(self.url + "csv/", 'GET')
        
    def data(self):
        """
        Get all the data of this series as a dictionary. Timetric
        doesn't document this particular API, so consider this method
        prone to change.
        """
        return simplejson.loads(self.client.request(self.url + 'json/', 'GET'))
        
    def __iter__(self):
        return (line for line in csv.reader(StringIO(self.csv())))
        
    def update(self, value):
        """
        Update the series. 
        
        The argument can be:
        
            * A single number: update the series with the given value and a
              timestamp of "now".
            
            * An iterator yielding ``(datetime, value)`` pairs: update the
              series with the given lines of data. Datetime values can be
              datetime objects, Unix timestamps, or strings capable of being
              parsed into a datetime using ``dateutil.parser`` (which
              understands most common formats).
              
            * A file-like object containing a stream of CSV data; this data
              will be fed directly to Timetric.
        
        """
        try:
            iter(value)
        except TypeError:
            self._update_single(value)
        else:
            if _is_file(value):
                self._update_from_file(value)
            else:
                self._update_from_iterable(value)
                            
    def delete(self):
        """
        Delete all data in the series. Doesn't actually remove the series;
        just empties it out.
        """
        self.client.request(self.url, 'DELETE')
        
    def _update_single(self, value):
        """
        Update the series with a single value and a timestamp of now.
        """
        self.client.request(self.url, 'POST', {'value': str(value)})
        
    def _update_from_file(self, file):
        """
        Update from a file-like object of CSV data.
        """
        self.client.request(self.url, 'POST', files={'csv': file})
        
    def _update_from_iterable(self, values):
        """
        Update from an iterable of 2-tuples (date, value)
        """
        io = StringIO()
        writer = csv.writer(io)
        for timestamp, value in values:
            writer.writerow([_parse_timestamp(timestamp), value])
        io.seek(0)
        self._update_from_file(io)

def _parse_timestamp(timestamp):
    """
    Parse a timestamp into a format that Timetric understands.
    """
    if hasattr(timestamp, 'timetuple'):
        return time.mktime(timestamp.utctimetuple())
    try:
        return float(timestamp)
    except (TypeError, ValueError):
        return time.mktime(dateutil.parser.parse(timestamp).utctimetuple())

#
# The following code is adapted from Django (django.test.client)
#
BOUNDARY = 'tHiSiStHeBoUnDaRyStRiNg'
MULTIPART_CONTENT = 'multipart/form-data; boundary=%s' % BOUNDARY

# Kind ghetto, but works.
def _is_file(thing): 
    return hasattr(thing, "read") and callable(thing.read)

def _encode_multipart(data, files):
    """
    Encodes multipart POST data from a dictionary of form values.
    """
    lines = []

    for (key, value) in data.items():
        lines.extend([
            '--' + BOUNDARY,
            'Content-Disposition: form-data; name="%s"' % str(key),
            '',
            str(value)
        ])
    for (key, value) in files.items():
        lines.extend([
            '--' + BOUNDARY,
            'Content-Disposition: form-data; name="%s"; filename="%s"' \
                % (str(key), str(key)),
            'Content-Type: application/octet-stream',
            '',
            value.read()
        ])

    lines.extend([
        '--' + BOUNDARY + '--',
        '',
    ])
    return '\r\n'.join(lines)
