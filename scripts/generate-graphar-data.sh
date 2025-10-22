set -euxo pipefail

# === Check graphar-cli installed ===
if ! command -v graphar &> /dev/null; then
    echo "graphar (cli) is required, please install it and retry" >&2
    exit 1
fi

# === Get Directory Paths ===
ROOTDIR=$(dirname "$(dirname "$(readlink -f "$0")")")

process_graph() {
    local GRAPH_DIR=$1
    local GRAPH_IMPORT_TEMPLATE=$GRAPH_DIR/import.script.git.yaml
    local GRAPH_IMPORT=$GRAPH_DIR/import.git.yaml
    local GRAPH_RESULT_DIR=$GRAPH_DIR/graphar

    # === Prepare files and directories to import ===
    mkdir -p "$GRAPH_RESULT_DIR"
    cp "$GRAPH_IMPORT_TEMPLATE" "$GRAPH_IMPORT"
    perl -i -pe "s|\\\$DIR_PATH|$GRAPH_DIR|g" "$GRAPH_IMPORT"

    # === Import data ===
    graphar import -c "$GRAPH_IMPORT"
    rm "$GRAPH_IMPORT"

    # === Create GraphInfo file ===
    local GRAPH_NAME=$(grep "name:" "$GRAPH_IMPORT_TEMPLATE" | head -n1 | sed 's/^[^:]*:[[:space:]]*//')
    local GRAPH_INFO_FILE=$GRAPH_RESULT_DIR/$GRAPH_NAME.graph.yaml

    local edges=($(find "$GRAPH_RESULT_DIR" -maxdepth 1 -type f -name "*edge*" -exec basename {} \;))
    local vertices=($(find "$GRAPH_RESULT_DIR" -maxdepth 1 -type f -name "*vertex*" -exec basename {} \;))

    {
        echo "edges:"
        for edge in "${edges[@]}"; do
            echo "  - $edge"
        done

        echo "name: $GRAPH_NAME"

        echo "vertices:"
        for vertex in "${vertices[@]}"; do
            echo "  - $vertex"
        done

        echo "version: gar/v1"
    } > "$GRAPH_INFO_FILE"
}

process_graph "$ROOTDIR/data/snap-musae-github"
process_graph "$ROOTDIR/data/snap-musae-github-csv"

echo "Successfully prepared test data."
