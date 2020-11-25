import bcrypt as bc
import time, sys, os, random

BASEPATH = os.path.join(os.sep, 'davide', 'home', 'userexternal', 'mberti00', 'Scripts', 'Crack')

if len(sys.argv) > 1 and int(sys.argv[1]):
    max_number = int(sys.argv[1])
    if len(sys.argv) > 2:
        if sys.argv[2] == 'random':
            # random word between 0 and max_number
            index = random.randrange(0, max_number, 1)
            iterator = 0
            for line in open(os.path.join(BASEPATH, 'passwords.txt'), 'r'):
                if iterator == index:
                    password = line.rstrip('\n')
                    break
                iterator += 1
        else:
            # password given by user
            password = sys.argv[2]
    else:
        # random generated word
        chrs = 'abcdefghijklmnopqrstuvwxyz0123456789'
        length = random.randrange(3, 7, 1)
        password = ''.join([chrs[int(random.random() * len(chrs))] for i in range(length)])
else:
    print("Required: maximum number of elements to check and password")
    sys.exit(1)

print("Check if \"" + password + "\" is within the " + str(max_number) + " most common passwords...")

salt = bc.gensalt()
password = bc.hashpw(password.encode('utf-8'), salt)

found = False
guesses = 0
start_time = time.time()

for line in open(os.path.join(BASEPATH, 'passwords.txt'), 'r'):
    if guesses < max_number:
        hashed = bc.hashpw(line.rstrip('\n').encode('utf-8'), salt)
        if password == hashed:
            guesses += 1
            found = True
            break
    else:
        break
    guesses += 1

print("Password found!" if found else "Password NOT found!")
print("Time: %fs" % (round(time.time() - start_time, 2)))
print("Guesses: " + str(guesses))
