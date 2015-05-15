# use vi on the command line
set -o vi

# environment variables - general
export EDITOR=vi
export LC_ALL=C
export SVN_EDITOR=vi

# environment variables - specific
export UNICYCLE_HOME=$HOME/unicycletrading
export TICKS_HOME=$HOME/unicycleticks
export ARCHIVE_HOME=$HOME/unicyclearchive
export CLASSPATH=$UNICYCLE_HOME/IBJts/java
export PATH=$PATH:$UNICYCLE_HOME/bin
export PERL5LIB=$UNICYCLE_HOME/lib

UNAME=`uname`
if [ "$UNAME" = "Linux" ]; then
    # add specific jar files to CLASSPATH
    JARFILE="/usr/share/java/mysql-connector-java.jar"    
    if [ -e "$JARFILE" ]; then
	export CLASSPATH=$CLASSPATH:$JARFILE
    fi
    JARFILE="$UNICYCLE_HOME/IBJts/java/jemmy.jar"
    if [ -e "$JARFILE" ]; then
	export CLASSPATH=$CLASSPATH:$JARFILE
    fi
    JARFILE="$UNICYCLE_HOME/IBJts/jts.jar"
    if [ -e "$JARFILE" ]; then
	export CLASSPATH=$CLASSPATH:$JARFILE
    fi
    JARFILE="$UNICYCLE_HOME/IBJts/language.jar"
    if [ -e "$JARFILE" ]; then
	export CLASSPATH=$CLASSPATH:$JARFILE
    fi
    JARFILE="$UNICYCLE_HOME/IBJts/total.2011.jar"
    if [ -e "$JARFILE" ]; then
	export CLASSPATH=$CLASSPATH:$JARFILE
    fi
    JARFILE="$UNICYCLE_HOME/IBJts/jtsclient.jar"
    if [ -e "$JARFILE" ]; then
	export CLASSPATH=$CLASSPATH:$JARFILE
    fi
    # add site-packages and $UNICYCLE_HOME/lib to PYTHONPATH
    PYTHONDIR="/usr/local/lib/python2.7/site-packages"
    if [ -d "$PYTHONDIR" ]; then
	export PYTHONPATH=$PYTHONDIR:$UNICYCLE_HOME/lib
    fi
    # sets a color prompt if COLOR_PROMPT=yes
    case "$TERM" in
	xterm-color) COLOR_PROMPT=no;;
    esac
    # to force color prompt, uncomment next line
    # FORCE_COLOR_PROMPT=yes
    if [ -n "$FORCE_COLOR_PROMPT" ]; then
	if [ -x /usr/bin/tput ] && tput setaf 1 >&/dev/null; then
	    COLOR_PROMPT=yes
	else
	    COLOR_PROMPT=
	fi
    fi
    # set PS1
    if [ "$COLOR_PROMPT" = yes ]; then
	PS1='\[\033[01;32m\]\u@\h\[\033[00m\]:\[\033[01;34m\]\w\[\033[00m\]\$ '
    else
	PS1='[\u@\h:\w]\n$ '
    fi
    unset COLOR_PROMPT FORCE_COLOR_PROMPT
    # if this is an xterm, set the window title to user@host: dir
    case "$TERM" in
	xterm*|rxvt*)
	    PS1="\[\e]0;\u@\h: \w\a\]$PS1"
	    ;;
	*)
	    ;;
    esac
    # enable color support for ls
    if [ -x /usr/bin/dircolors ]; then
	test -r ~/.dircolors && eval "$(dircolors -b ~/.dircolors)" || eval "$(dircolors -b)"
    fi
fi
unset JARFILE PYTHONDIR UNAME

# don't duplicate lines in history, see bash(1) for more options
HISTCONTROL=ignoredups:ignorespace

# set history length, see bash(1)
HISTSIZE=1000
HISTFILESIZE=2000

# append history
if [ -n "$PROMPT_COMMAND" ]; then
    PROMPT_COMMAND="$PROMPT_COMMAND; history -a"
else
    PROMPT_COMMAND='history -a'
fi

# alias definitions
if [ -f ~/.bash_aliases ]; then
    . ~/.bash_aliases
fi
