CC=g++
CXXFLAGS=-std=c++11 -Wall -O2 -I./ -pthread
SRC=$(wildcard *.cpp)
OBJ=$(patsubst %.cpp,%.o,$(SRC))
TARGET=skiplist

all:$(TARGET)

$(OBJ):$(SRC)
	$(CC) $(CXXFLAGS) -c $< -o $@

$(TARGET):$(OBJ)
	$(CC) $(CXXFLAGS) -o $@ $^
	rm -f ./*.o

clean: 
	rm -f ./*.o
