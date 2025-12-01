import bcrypt 
import getpass
password = getpass.getpass(prompt="Enter a strong password: ")
password = password.encode("utf-8")
hashed = bcrypt.hashpw(password, bcrypt.gensalt())
print(hashed.decode())