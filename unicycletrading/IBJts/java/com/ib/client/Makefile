SRC := $(wildcard *.java)
CLASSES := $(SRC:.java=.class)

JAVAFLAGS = -classpath ../../..

all: $(CLASSES)

%.class: %.java
	javac $(JAVAFLAGS) $*.java
