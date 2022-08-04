from poster.encode import multipart_encode
from poster.streaminghttp import register_openers
import urllib2

# Register the streaming http handlers with urllib2
register_openers()

# Use multipart encoding for the input files
datagen, headers = multipart_encode({ 'files[]': open('test.fdb', 'rb')})

# Create the request object
request = urllib2.Request('https://www.rebasedata.com/api/v1/convert', datagen, headers)

# Do the request and get the response
# Here the FDB file gets converted to CSV
response = urllib2.urlopen(request)

# Check if an error came back
if response.info().getheader('Content-Type') == 'application/json':
    print (response.read())
    sys.exit(1)

# Write the response to /tmp/output.zip
with open('output.zip', 'wb') as local_file:
    local_file.write(response.read())

print ('Conversion result successfully written to output.zip!')
