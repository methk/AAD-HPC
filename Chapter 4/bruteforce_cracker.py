import bcrypt as bc
import time, sys, random, itertools

if len(sys.argv) > 1:
    # password given by user
    password = sys.argv[1]
else:
    # random generated word
    chrs = 'abcdefghijklmnopqrstuvwxyz0123456789'
    length = random.randrange(3, 7, 1)
    password = ''.join([chrs[int(random.random() * len(chrs))] for i in range(length)])

print("Bruteforce on \"" + password + "\"...")

salt = bc.gensalt()
password = bc.hashpw(password.encode('utf-8'), salt)

found = False
guesses = 0
start_time = time.time()

for pswd_length in range(1, 7, 1):
    for guess in itertools.product('abcdefghijklmnopqrstuvwxyz0123456789', repeat=pswd_length):
        guesses += 1
        guess = ''.join(guess)
        hashed = bc.hashpw(guess.encode('utf-8'), salt)
        if password == hashed:
            found = True
            break
    if found:
        break

print("Password found!" if found else "Password NOT found!")
print("Time: %fs" % (round(time.time() - start_time, 2)))
print("Guesses: " + str(guesses))
