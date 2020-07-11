declare -A DATASETS=( ["tank/Public"]="dozer/Public" 
                      ["tank/Users"]="dozer/Users"
		      )

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
PEER=10.0.80.80
PEER_USER=root

SNAPSHOT_PREFIX=rep

function log
{
   >&2 echo $@
}

function remote_command
{
   local REMOTECOMMAND="ssh $PEER_USER@$PEER \"$@\""
   log $REMOTECOMMAND
   eval $REMOTECOMMAND
}

function local_background_command
{
   local COMMAND=$@
   log $COMMAND
   eval $COMMAND &
}

function create_replication_snapshot
{
    local SNAPSHOT_NAME=$SNAPSHOT_PREFIX$TIMESTAMP
    local COMMAND="zfs snapshot $1@$SNAPSHOT_NAME"
    remote_command $COMMAND

    echo $SNAPSHOT_NAME
}

function create_bookmark_from_snapshot
{
    local COMMAND="zfs bookmark $1@$2 $1#$2"
    remote_command $COMMAND
}

function delete_replication_snapshot
{
    local COMMAND="zfs destroy $1@$2"
    remote_command $COMMAND
}

function perform_full_replication
{
    log "Performing full replication from $1 to $2..."
    local SNAPSHOT_NAME="$(create_replication_snapshot $1 $SNAPSHOT_NAME)"

    local_background_command "(nc -l -p 8024 | /opt/tools/bin/mbuffer -q -s 32M -m 1G | pv -rtab | zfs receive $2)"
    remote_command "zfs send $1@$SNAPSHOT_NAME | mbuffer -q -s 32M -m 1G | nc -w 600 10.99.50.50 8024"

    create_bookmark_from_snapshot $1 $SNAPSHOT_NAME
    delete_replication_snapshot $1 $SNAPSHOT_NAME

    log "Replication from $1 to $2 complete."
}

function perform_incremental_replication
{
    log "Performing incremental replication from $1 to $2 using bookmark $BOOKMARK..."
}

# Loop through all datasets
for KEY in "${!DATASETS[@]}"; do
    log "Replicating $KEY to ${DATASETS[$KEY]}"

    # First, get the most recent snapshot bookmark (if any)
    BOOKMARKS=$(ssh $PEER_USER@$PEER "zfs list -t bookmark -H | grep $KEY | sort -r | head -1 | cut -f1")
    if [[ -z "$BOOKMARKS" ]]; then
        perform_full_replication $KEY ${DATASETS[$KEY]}
    else
        perform_incremental_replication $KEY ${DATASETS[$KEY]} $BOOKMARK
    fi

    # Kill any background jobs
    kill $(jobs -p)
done

