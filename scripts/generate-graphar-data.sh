set -euxo pipefail

# === Check graphar-cli installed ===
if ! command -v graphar &> /dev/null; then
    echo "graphar (cli) is required, please install it and retry" >&2
    exit 1
fi

# === Get Directory Paths ===
ROOTDIR=$(dirname "$(dirname "$(readlink -f "$0")")")

GRAPH_DIR=$ROOTDIR/data/snap-musae-github
GRAPH_IMPORT_TEMPLATE=$GRAPH_DIR/import.script.git.yaml
GRAPH_IMPORT=$GRAPH_DIR/import.git.yaml
GRAPH_RESULT_DIR=$GRAPH_DIR/graphar

# === Prepare files and directories to import ===
mkdir -p "$GRAPH_RESULT_DIR"
cp "$GRAPH_IMPORT_TEMPLATE" "$GRAPH_IMPORT"
sed -i "s|\$DIR_PATH|$GRAPH_DIR|g" "$GRAPH_IMPORT"

# === Import data ===
graphar import -c "$GRAPH_IMPORT"
rm "$GRAPH_IMPORT"

# === Create GraphInfo file ===
GRAPH_NAME=$(grep "name:" "$GRAPH_IMPORT_TEMPLATE" | head -n1 | sed 's/^[^:]*:[[:space:]]*//')
GRAPH_INFO_FILE=$GRAPH_RESULT_DIR/$GRAPH_NAME.graph.yaml

edges=$(find "$GRAPH_RESULT_DIR" -maxdepth 1 -type f -name "*edge*" -exec basename {} \;)
vertices=$(find "$GRAPH_RESULT_DIR" -maxdepth 1 -type f -name "*vertex*" -exec basename {} \;)

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