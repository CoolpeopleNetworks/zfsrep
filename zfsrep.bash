declare -A DATASETS=( ["root@10.99.80.80:tank/backups"]="tank/backups/servers"
                      ["root@10.99.80.80:tank/ipxe"]="tank/backups/ipxe"
		      ["root@10.99.80.80:tank/tftpboot"]="tank/backups/tftpboot"
                      ["root@10.99.80.80:tank/Media"]="tank/backups/Media"
                      ["root@10.99.80.80:tank/Public"]="tank/backups/Public" 
                      ["root@10.99.80.80:tank/Users"]="tank/backups/Users"
		      ["root@10.99.80.80:tank/Photography"]="tank/backups/Photography"
		      )

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
PEER=""

SNAPSHOT_PREFIX=rep

function log
{
   >&2 echo $@
}

function local_command
{
    local LOCAL_COMMAND=$@
    log "[LOCAL] $LOCAL_COMMAND"
    eval $LOCAL_COMMAND
}

function remote_command
{
    local REMOTECOMMAND="ssh $PEER \"$@\""
    log "[REMOTE: $PEER] $@"
    eval $REMOTECOMMAND
}

function create_replication_snapshot
{
    local SNAPSHOT_NAME=$SNAPSHOT_PREFIX$TIMESTAMP
    local COMMAND="zfs snapshot -r $1@$SNAPSHOT_NAME"
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
    local COMMAND="zfs destroy -r $1@$2"
    remote_command $COMMAND
}

function delete_replication_bookmark
{
    local COMMAND="zfs destroy $1"
    remote_command $COMMAND
}

function perform_full_replication
{
    log "Performing full replication from $1 to $2..."
    SNAPSHOT_DATASET=$1
    SNAPSHOT_NAME="$(create_replication_snapshot $SNAPSHOT_DATASET)"

    local COMMAND="ssh $PEER -c aes128-ctr -m hmac-sha1 \"zfs send -R $SNAPSHOT_DATASET@$SNAPSHOT_NAME \" | pv -rtab | zfs receive $2"
    local_command $COMMAND
    if [ $? -ne 0 ]; then
        log "Failed to replicate $1 - exit code $?."
        delete_replication_snapshot $SNAPSHOT_DATASET $SNAPSHOT_NAME
	exit
    fi

    create_bookmark_from_snapshot $SNAPSHOT_DATASET $SNAPSHOT_NAME
    delete_replication_snapshot $SNAPSHOT_DATASET $SNAPSHOT_NAME

    log "Replication from $1 to $2 complete."
}

function perform_incremental_replication
{
    log "Performing incremental replication from $1 to $2 using bookmark $3..."
    SNAPSHOT_DATASET=$1
    SNAPSHOT_NAME="$(create_replication_snapshot $SNAPSHOT_DATASET)"
    BOOKMARK=$3

    local COMMAND="ssh $PEER -c aes128-ctr -m hmac-sha1 \"zfs send -i $BOOKMARK $SNAPSHOT_DATASET@$SNAPSHOT_NAME \" | pv -rtab | zfs receive -F $2"
    local_command $COMMAND
    if [ $? -ne 0 ]; then
        log "Failed to replicate $1 - exit code $?."
        delete_replication_snapshot $SNAPSHOT_DATASET $SNAPSHOT_NAME
	exit
    fi

    delete_replication_bookmark $BOOKMARK
    create_bookmark_from_snapshot $SNAPSHOT_DATASET $SNAPSHOT_NAME
    delete_replication_snapshot $SNAPSHOT_DATASET $SNAPSHOT_NAME

    log "Incremental replication from $1 to $2 complete."
}

function cleanup
{
    log "\nCTRL-C detected...deleting snapshot..."
    delete_replication_snapshot $SNAPSHOT_DATASET $SNAPSHOT_NAME 
}

trap 'cleanup; exit' INT

# Loop through all datasets
for KEY in "${!DATASETS[@]}"; do
    log "Replicating $KEY to ${DATASETS[$KEY]}"

    IFS=':'
    read -r -a PEERSPEC <<< "$KEY"
    PEER=${PEERSPEC[0]}
    DATASET=${PEERSPEC[1]}

    # First, get the most recent snapshot bookmark (if any)
    BOOKMARK=$(ssh $PEER "zfs list -t bookmark -H | grep $DATASET | sort -r | head -1 | cut -f1")
    if [[ -z "$BOOKMARK" ]]; then
        perform_full_replication $DATASET ${DATASETS[$KEY]}
    else
        perform_incremental_replication $DATASET ${DATASETS[$KEY]} $BOOKMARK
    fi
done

