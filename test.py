import hashlib

input_str = "123"
hashed_str = hashlib.md5(input_str.encode()).hexdigest()

print(hashed_str)
