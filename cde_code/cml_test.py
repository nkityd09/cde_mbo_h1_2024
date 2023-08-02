# import cmlapi
# client = cmlapi.default_client(url="https://ml-bf023b02-636.mbo-demo.a465-9q4k.cloudera.site", cml_api_key="985505643c23b944d7fe678bfcbf4c00130312010cbe9b5db66d1a5f6d04b218.5e4cdf24c8429b288600b16122765ec2fbcb9f280e0f959d656a282926c44380")
# client.list_projects()
   

import cmlapi
api_url= "https://ml-bf023b02-636.mbo-demo.a465-9q4k.cloudera.site"
api_key= "985505643c23b944d7fe678bfcbf4c00130312010cbe9b5db66d1a5f6d04b218.5e4cdf24c8429b288600b16122765ec2fbcb9f280e0f959d656a282926c44380"
# No arguments are required when the default_client method is used inside a session.
api_client=cmlapi.default_client(url=api_url,cml_api_key=api_key)
api_client.list_projects()   