# Lichess Analysis — shell aliases
# Source this in ~/.zshrc:
#   source /Users/joshcrasto/Projects/lichess_analysis/v2/deploy/command_aliases.sh

LICHESS_DOMAIN="gui/$(id -u)"
LICHESS_LABEL="com.joshcrasto.lichess_analysis"
LICHESS_CONF="/Users/joshcrasto/Projects/lichess_analysis/v2/deploy/supervisord.conf"

alias startchess='launchctl kickstart '"$LICHESS_DOMAIN/$LICHESS_LABEL"
alias stopchess='launchctl kill SIGTERM '"$LICHESS_DOMAIN/$LICHESS_LABEL"
alias restartchess='launchctl kill SIGTERM '"$LICHESS_DOMAIN/$LICHESS_LABEL"' && sleep 2 && launchctl kickstart '"$LICHESS_DOMAIN/$LICHESS_LABEL"
alias chess_status='supervisorctl -c '"$LICHESS_CONF"' status'
alias clean_chess_logs='rm /Users/joshcrasto/Projects/lichess_analysis/v2/logs/*.log'
