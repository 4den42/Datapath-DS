CC      ?= cc
CFLAGS  ?= -std=c11 -Wall -Wextra -Wpedantic -O2 -g
LDLIBS  = -lpthread
SRCS    = driver.c flowtable.c
HDRS    = ring.h flowtable.h

.PHONY: all run asan tsan check clean

all: driver

driver: $(SRCS) $(HDRS)
	$(CC) $(CFLAGS) -o $@ $(SRCS) $(LDLIBS)

# AddressSanitizer + UndefinedBehaviorSanitizer: memory errors, overflow, UB.
driver-asan: $(SRCS) $(HDRS)
	$(CC) $(CFLAGS) -O1 -fsanitize=address,undefined -fno-omit-frame-pointer -o $@ $(SRCS) $(LDLIBS)

# ThreadSanitizer: data races between producer and consumer.
driver-tsan: $(SRCS) $(HDRS)
	$(CC) $(CFLAGS) -O1 -fsanitize=thread -o $@ $(SRCS) $(LDLIBS)

run: driver
	./driver

asan: driver-asan
	./driver-asan test --quick && ./driver-asan pipeline --quick

tsan: driver-tsan
	./driver-tsan test --quick && ./driver-tsan pipeline --quick

check: run asan tsan

clean:
	rm -f driver driver-asan driver-tsan
