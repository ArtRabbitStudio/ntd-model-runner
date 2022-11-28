import base64

def b64encode( utf8_str ):
    return base64.b64encode( bytes( utf8_str, 'utf-8' ) ).decode('utf-8')
