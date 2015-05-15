SRC := $(wildcard *.java)
CLASSES := $(SRC:.java=.class)

JAVAFLAGS = -classpath ..:../jemmy.jar

all: $(CLASSES)

%.class: %.java
	javac $(JAVAFLAGS) $*.java

clean:
	rm -f *.class
