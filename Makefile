CC=gcc
CXX=g++
RM=rm -rf
C_FILES := $(wildcard *.c)
OBJS := $(addprefix obj/,$(notdir $(C_FILES:.c=.o)))


CFLAGS=-g -O0 --coverage -Wall -DDEBUG
CPPFLAGS=-g -O0 --coverage -Wall
LDFLAGS=-g -O0 --coverage  -DDEBUG
LDLIBS= -lm

all: main

main: $(OBJS)
	    $(CC) $(LDFLAGS) -o main $(OBJS) $(LDLIBS) 

obj/%.o: %.c
	   $(CC) -c $(CFLAGS) -o $@ $<
clean:
	    $(RM) $(OBJS)

dist-clean: clean
	    $(RM) main out obj/*
