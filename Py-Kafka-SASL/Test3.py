import threading
import time


# Function representing the infinite loop
def infinite_loop(name, delay, run_event):
    counter = 0
    while run_event.is_set():
        counter = counter + 1
        print(name, "Looping", str(counter))
        time.sleep(delay)


# Function to take input from the user
def take_input(name, run_event):
    while run_event.is_set():
        user_input = input("Put input:")
        print("Enter", name, ":", user_input)


run_event = threading.Event()
run_event.set()

# Create two threads, one for the infinite loop and one for taking input
conversation_thread = threading.Thread(target=infinite_loop, args=("message", 2, run_event))
input_thread = threading.Thread(target=take_input, args=("input", run_event))

# Start both threads
conversation_thread.start()
input_thread.start()

try:
    while 1:
        time.sleep(.1)
except KeyboardInterrupt:
    print("attempting to close threads")
    run_event.clear()
    # Wait for both threads to finish
    conversation_thread.join()
    input_thread.join()
    print("threads successfully closed")
