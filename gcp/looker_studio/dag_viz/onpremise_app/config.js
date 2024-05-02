const options = {
    width: '100%',
    height: '100%',
    autoResize: true,
    "nodes": {
        "borderWidthSelected": 2,
        "shape": "box",
        "font": "20px monospace white",
        "color": "rgba(0, 0, 0, 0.8)",
        "chosen": {
            "node": (values, id, selected, hovering) => {
                values.borderColor = values.color !== 'rgba(0, 0, 0, 0.8)' ? "black" : "yellow";
                if (selected) {
                    values.borderWidth = 4;
                }
            }
        },
    },
    "edges": {
        "color": {
            "inherit": true
        },
        "smooth": {
            "enabled": true,
            "type": "cubicBezier",
            "forceDirection": "vertical"
        },
        "arrows":{
            "to": true,
        }
    },
    "interaction": {
        "hover": true,
        "hoverConnectedEdges": false,
        "multiselect": false,
        "tooltipDelay": 0,
    },
    "layout": {
        "hierarchical": {
            "blockShifting": true,
            "edgeMinimization": true,
            "enabled": true,
            "levelSeparation": 150,
            "parentCentralization": true,
            "sortMethod": "directed",
            "treeSpacing": 200,
            "nodeSpacing": 250,
            "sortMethod": "directed",
        },
        "improvedLayout": true,
        "randomSeed": 0
    },
    "physics": {
        "enabled": false,
        "hierarchicalRepulsion": {
            "avoidOverlap": 1
        }
    },
    "manipulation": {
        "enabled": false,
    },
    "configure": {
        "enabled": false,
        "filter": "nodes",
    }

};

module.exports = options;