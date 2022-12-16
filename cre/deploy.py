import os
import sys

#os.system("mv %s /usr/share/nginx/html/" % sys.argv[1])
test = 0
for arg in sys.argv:
   if "--test" in arg.lower():
      test = 1

if test:
    for item in [filen for filen in sys.argv[1:] if not "--" in filen]:
        print("deploying %s into test folder" % item)
        os.system("sudo rm -rf /usr/share/nginx/html/tilesets/test/%s " % item.replace(".tgz",""))
        os.system("sudo tar xf %s -C /usr/share/nginx/html/tilesets/test" % item)
        os.system("sudo chown -R nginx:nginx /usr/share/nginx/html/tilesets/test/%s" % item.replace(".tgz",""))
        os.system("sudo rm -rf /var/cache/nginx/*")
        os.system("sudo ls -ltrh /usr/share/nginx/html/tilesets/test/ | grep %s" % (item.replace(".tgz","")))
        print("finished")
else:
    for item in [filen for filen in sys.argv[1:] if not "--" in filen]:
        print("deploying %s into production folder" % item)
        os.system("sudo rm -rf /usr/share/nginx/html/tilesets/old/%s " % item.replace(".tgz",""))
        os.system("sudo mv /usr/share/nginx/html/tilesets/%s /usr/share/nginx/html/tilesets/old " % item.replace(".tgz",""))
        os.system("sudo tar xf %s -C /usr/share/nginx/html/tilesets" % item)
        os.system("sudo chown -R nginx:nginx /usr/share/nginx/html/tilesets/%s" % item.replace(".tgz",""))
        os.system("sudo rm -rf /var/cache/nginx/*")
        os.system("sudo ls -ltrh /usr/share/nginx/html/tilesets/ | grep %s" % (item.replace(".tgz","")))
        print("finished")
