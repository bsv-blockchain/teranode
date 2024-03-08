import * as d3 from 'd3';
import type {HierarchyNode, TreeLayout} from "d3-hierarchy";

function treeBoxes(selector, jsonData)
{
    const blue = '#337ab7',
        green = '#5cb85c',
        yellow = '#f0ad4e',
        // blueText = '#4ab1eb',
        purple = '#9467bd';

    const margin = {
            top : 0,
            right : 0,
            bottom : 100,
            left : 0
        };
    // Height and width are redefined later in function of the size of the tree
    // (after that the data are loaded)
    let width = 800 - margin.right - margin.left,
        height = 400 - margin.top - margin.bottom;

    const rectNode = { width : 120, height : 45, textMargin : 5 },
        tooltip = { width : 150, height : 40, textMargin : 5 },
        duration = 750;
    let i = 0;

    let mousedown,
        mouseWheelName; // Use to save temporarily 'mousedown.zoom' value
    // let mouseWheel,
    //     mouseWheelName,
    //     isKeydownZoom = false;

    let tree: TreeLayout<any>;
    let root: HierarchyNode<any>;
    let baseSvg,
        svgGroup,
        nodeGroup, // If nodes are not grouped together, after a click the svg node will be set after his corresponding tooltip and will hide it
        nodeGroupTooltip,
        linkGroup,
        linkGroupToolTip,
        defs;

    init(jsonData);

    function init(jsonData)
    {
        if (jsonData) {
            drawTree(jsonData);
        } else {
            console.error(jsonData);
            alert('Invalid data.');
        }
    }

    function drawTree(jsonData)
    {
        root = d3.hierarchy(jsonData);
        tree = d3.tree().size([ height, width ]);
        // const nodes = d3.hierarchy(jsonData, function(d) {
        //     return d.children;
        // });

        // Dynamically set the height of the main svg container
        // breadthFirstTraversal returns the max number of node on a same level
        // and colors the nodes
        let maxDepth = 0;
        const maxTreeWidth = breadthFirstTraversal(root.children, function(currentLevel) {
            maxDepth++;
            currentLevel.forEach(function(node) {
                if (node.type == 'type1')
                    node.color = blue;
                if (node.type == 'type2')
                    node.color = green;
                if (node.type == 'type3')
                    node.color = yellow;
                if (node.type == 'type4')
                    node.color = purple;
            });
        });
        height += maxTreeWidth * (rectNode.height + 20) + tooltip.height + 20 - margin.right - margin.left;
        width += maxDepth * (rectNode.width * 1.5) + tooltip.width / 2 - margin.top - margin.bottom;

        console.log({maxDepth, maxTreeWidth, height, width});

        tree = d3.tree().size([ height, width ]);
        root.x0 = height / 2;
        root.y0 = 0;

        baseSvg = d3.select(selector).append('svg')
            .attr('width', width + margin.right + margin.left)
            .attr('height', height + margin.top + margin.bottom)
            .attr('class', 'svgContainer')
            //.call(d3.zoom().on('zoom', zoomAndDrag));
        //.scaleExtent([0.5, 1.5]) // Limit the zoom scale

        // Mouse wheel is desactivated, else after a first drag of the tree, wheel event drags the tree (instead of scrolling the window)
        getMouseWheelEvent();
        d3.select(selector).select('svg').on(mouseWheelName, null);
        d3.select(selector).select('svg').on('dblclick.zoom', null);

        svgGroup = baseSvg.append('g')
            .attr('class','drawarea')
            .append('g')
            .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');

        // SVG elements under nodeGroupTooltip could be associated with nodeGroup,
        // same for linkGroupToolTip and linkGroup,
        // but this separation allows to manage the order on which elements are drew
        // and so tooltips are always on top.
        nodeGroup = svgGroup.append('g')
            .attr('id', 'nodes');
        linkGroup = svgGroup.append('g')
            .attr('id', 'links');
        linkGroupToolTip = svgGroup.append('g')
            .attr('id', 'linksTooltips');
        nodeGroupTooltip = svgGroup.append('g')
            .attr('id', 'nodesTooltips');

        defs = baseSvg.append('defs');
        initArrowDef();
        initDropShadow();

        update(root);
    }

    function update(source)
    {
        console.log({source})

        // Compute the new tree layout
        const nodes = root.descendants().reverse();
        const links = tree(root).links();

        // Check if two nodes are in collision on the ordinates axe and move them
        breadthFirstTraversal(root.children, collision);
        // Normalize for fixed-depth
        nodes.forEach(function(d) {
            d.y = d.depth * (rectNode.width * 1.5);
        });

        // 1) ******************* Update the nodes *******************
        const node = nodeGroup.selectAll('g.node').data(nodes, function(d) {
            return d.id || (d.id = ++i);
        });
        const nodesTooltip = nodeGroupTooltip.selectAll('g').data(nodes, function(d) {
            return d.id || (d.id = ++i);
        });

        // Enter any new nodes at the parent's previous position
        // We use "insert" rather than "append", so when a new child node is added (after a click)
        // it is added at the top of the group, so it is drawed first
        // else the nodes tooltips are drawed before their children nodes and they
        // hide them
        const nodeEnter = node.enter().insert('g', 'g.node')
            .attr('class', 'node')
            .attr('transform', function() {
                return 'translate(' + source.y0 + ',' + source.x0 + ')'; })
            .on('click', function(d) {
                click(d);
            });
        const nodeEnterTooltip = nodesTooltip.enter().append('g')
            .attr('transform', function() {
                return 'translate(' + source.y0 + ',' + source.x0 + ')'; });

        nodeEnter.append('g').append('rect')
            .attr('rx', 6)
            .attr('ry', 6)
            .attr('width', rectNode.width)
            .attr('height', rectNode.height)
            .attr('class', 'node-rect')
            .attr('fill', function (d) { return d.color; })
            .attr('filter', 'url(#drop-shadow)');

        nodeEnter.append('foreignObject')
            .attr('x', rectNode.textMargin)
            .attr('y', rectNode.textMargin)
            .attr('width', function() {
                return (rectNode.width - rectNode.textMargin * 2) < 0 ? 0
                    : (rectNode.width - rectNode.textMargin * 2)
            })
            .attr('height', function() {
                return (rectNode.height - rectNode.textMargin * 2) < 0 ? 0
                    : (rectNode.height - rectNode.textMargin * 2)
            })
            .append('xhtml').html(function(d) {
                return '<div style="width: '
                    + (rectNode.width - rectNode.textMargin * 2) + 'px; height: '
                    + (rectNode.height - rectNode.textMargin * 2) + 'px;" class="node-text wordwrap">'
                    + '<b>Height: </b>' + d.data.version + '<br>'
                    + '<b>' + d.data.name + '</b><br><br>'
                    + '</div>';
            })
            // .on('mouseover', function() {
            //     // TODO fix this for Svelte
            //     // $('#nodeInfoID' + d.id).css('visibility', 'visible');
            //     // $('#nodeInfoTextID' + d.id).css('visibility', 'visible');
            // })
            // .on('mouseout', function() {
            //     // $('#nodeInfoID' + d.id).css('visibility', 'hidden');
            //     // $('#nodeInfoTextID' + d.id).css('visibility', 'hidden');
            // });

        nodeEnterTooltip.append("rect")
            .attr('id', function(d) { return 'nodeInfoID' + d.id; })
            .attr('x', rectNode.width / 2)
            .attr('y', rectNode.height / 2)
            .attr('width', tooltip.width)
            .attr('height', tooltip.height)
            .attr('class', 'tooltip-box')
            .style('fill-opacity', 0.8)
            .on('mouseover', function() {
                // $('#nodeInfoID' + d.id).css('visibility', 'visible');
                // $('#nodeInfoTextID' + d.id).css('visibility', 'visible');
                removeMouseEvents();
            })
            .on('mouseout', function() {
                // $('#nodeInfoID' + d.id).css('visibility', 'hidden');
                // $('#nodeInfoTextID' + d.id).css('visibility', 'hidden');
                reactivateMouseEvents();
            });

        nodeEnterTooltip.append("text")
            .attr('id', function(d) { return 'nodeInfoTextID' + d.id; })
            .attr('x', rectNode.width / 2 + tooltip.textMargin)
            .attr('y', rectNode.height / 2 + tooltip.textMargin * 2)
            .attr('width', tooltip.width)
            .attr('height', tooltip.height)
            .attr('class', 'tooltip-text')
            .style('fill', 'white')
            .append("tspan")
            .text(function(d) {return 'Name: ' + d.data.name;})
            .append("tspan")
            .attr('x', rectNode.width / 2 + tooltip.textMargin)
            .attr('dy', '1.5em')
            .text(function(d) {return 'Info: ' + d.data.label;});

        // Transition nodes to their new position.
        const nodeUpdate = node.transition().duration(duration)
            .attr('transform', function(d) { return 'translate(' + d.y + ',' + d.x + ')'; });
        nodesTooltip.transition().duration(duration)
            .attr('transform', function(d) { return 'translate(' + d.y + ',' + d.x + ')'; });

        nodeUpdate.select('rect')
            .attr('class', function(d) { return d._children ? 'node-rect-closed' : 'node-rect'; });

        nodeUpdate.select('text').style('fill-opacity', 1);

        // Transition exiting nodes to the parent's new position
        const nodeExit = node.exit().transition().duration(duration)
            .attr('transform', function() { return 'translate(' + source.y + ',' + source.x + ')'; })
            .remove();
        nodesTooltip.exit().transition().duration(duration)
            .attr('transform', function() { return 'translate(' + source.y + ',' + source.x + ')'; })
            .remove();

        nodeExit.select('text').style('fill-opacity', 1e-6);


        // 2) ******************* Update the links *******************
        const link = linkGroup.selectAll('path').data(links, function(d) {
            return d.target.id;
        });
        const linkTooltip = linkGroupToolTip.selectAll('g').data(links, function(d) {
            return d.target.id;
        });

        function linkMarkerStart(direction, isSelected) {
            if (direction == 'SYNC')
            {
                return isSelected ? 'url(#start-arrow-selected)' : 'url(#start-arrow)';
            }
            return '';
        }

        function linkType(link) {
            if (link.direction == 'SYNC')
                return "Synchronous [\u2194]";
            else
            {
                if (link.direction == 'ASYN')
                    return "Asynchronous [\u2192]";
            }
            return '???';
        }

        d3.selection.prototype.moveToFront = function() {
            return this.each(function(){
                // @ts-expect-error ignore
                this.parentNode.appendChild(this);
            });
        };

        // Enter any new links at the parent's previous position.
        // Enter any new links at the parent's previous position.
        const linkenter = link.enter().insert('path', 'g')
            .attr('class', 'link')
            .attr('id', function(d) { return 'linkID' + d.target.id; })
            .attr('d', function(d) { return diagonal(d); })
            .attr('marker-end', 'url(#end-arrow)')
            .attr('marker-start', function(d) { return linkMarkerStart(d.target.data.link.direction, false); })
            // .on('mouseover', function(d) {
            //     //d3.select(this).moveToFront();
            //
            //     d3.select(this).attr('marker-end', 'url(#end-arrow-selected)');
            //     d3.select(this).attr('marker-start', linkMarkerStart(d.target.data.link.direction, true));
            //     d3.select(this).attr('class', 'linkselected');
            //
            //     // $('#tooltipLinkID' + d.target.id).attr('x', (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y);
            //     // $('#tooltipLinkID' + d.target.id).attr('y', (d.target.x - d.source.x) / 2 + d.source.x);
            //     // $('#tooltipLinkID' + d.target.id).css('visibility', 'visible');
            //     // $('#tooltipLinkTextID' + d.target.id).css('visibility', 'visible');
            // })
            // .on('mouseout', function(d) {
            //     d3.select(this).attr('marker-end', 'url(#end-arrow)');
            //     d3.select(this).attr('marker-start', linkMarkerStart(d.target.data.link.direction, false));
            //     d3.select(this).attr('class', 'link');
            //     // $('#tooltipLinkID' + d.target.id).css('visibility', 'hidden');
            //     // $('#tooltipLinkTextID' + d.target.id).css('visibility', 'hidden');
            // });

        linkTooltip.enter().append('rect')
            .attr('id', function(d) { return 'tooltipLinkID' + d.target.id; })
            .attr('class', 'tooltip-box')
            .style('fill-opacity', 0.8)
            .attr('x', function(d) { return (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y; })
            .attr('y', function(d) { return (d.target.x - d.source.x) / 2 + d.source.x; })
            .attr('width', tooltip.width)
            .attr('height', tooltip.height)
            .on('mouseover', function() {
                // $('#tooltipLinkID' + d.target.id).css('visibility', 'visible');
                // $('#tooltipLinkTextID' + d.target.id).css('visibility', 'visible');
                // After selected a link, the cursor can be hover the tooltip, that's why we still need to highlight the link and the arrow
                // $('#linkID' + d.target.id).attr('class', 'linkselected');
                // $('#linkID' + d.target.id).attr('marker-end', 'url(#end-arrow-selected)');
                // $('#linkID' + d.target.id).attr('marker-start', linkMarkerStart(d.target.link.direction, true));

                removeMouseEvents();
            })
            .on('mouseout', function() {
                // $('#tooltipLinkID' + d.target.id).css('visibility', 'hidden');
                // $('#tooltipLinkTextID' + d.target.id).css('visibility', 'hidden');
                // $('#linkID' + d.target.id).attr('class', 'link');
                // $('#linkID' + d.target.id).attr('marker-end', 'url(#end-arrow)');
                // $('#linkID' + d.target.id).attr('marker-start', linkMarkerStart(d.target.link.direction, false));

                reactivateMouseEvents();
            });

        linkTooltip.enter().append('text')
            .attr('id', function(d) { return 'tooltipLinkTextID' + d.target.id; })
            .attr('class', 'tooltip-text')
            .attr('x', function(d) { return (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y + tooltip.textMargin; })
            .attr('y', function(d) { return (d.target.x - d.source.x) / 2 + d.source.x + tooltip.textMargin * 2; })
            .attr('width', tooltip.width)
            .attr('height', tooltip.height)
            .style('fill', 'white')
            .append("tspan")
            .text(function(d) { return linkType(d.target); })
            .append("tspan")
            .attr('x', function(d) { return (d.target.y + rectNode.width - d.source.y) / 2 + d.source.y + tooltip.textMargin; })
            .attr('dy', '1.5em')
            .text(function(d) {return d.target.data.name;});

        // Transition links to their new position.
        const linkUpdate = link.transition().duration(duration)
             .attr('d', function(d) { return diagonal(d); });
        linkTooltip.transition().duration(duration)
            .attr('d', function(d) { return diagonal(d); });

        // Transition exiting nodes to the parent's new position.
        link.exit().transition()
            .remove();

        linkTooltip.exit().transition()
            .remove();

        // Stash the old positions for transition.
        nodes.forEach(function(d) {
            d.x0 = d.x;
            d.y0 = d.y;
        });
    }

    // Zoom functionnality is desactivated (user can use browser Ctrl + mouse wheel shortcut)
    // function zoomAndDrag(event) {
    //     console.log({event})
    //     //var scale = d3.event.scale,
    //     const scale = 1,
    //         tbound = -height * scale,
    //         bbound = height * scale,
    //         lbound = (-width + margin.right) * scale,
    //         rbound = (width - margin.left) * scale;
    //     let translation = d3.event.translate;
    //     // limit translation to thresholds
    //     console.log({translation})
    //     translation = [
    //         Math.max(Math.min(translation[0], rbound), lbound),
    //         Math.max(Math.min(translation[1], bbound), tbound)
    //     ];
    //     d3.select('.drawarea')
    //         .attr('transform', 'translate(' + translation + ')' +
    //             ' scale(' + scale + ')');
    // }

    // Toggle children on click.
    function click(d) {
        if (d.children) {
            d._children = d.children;
            d.children = null;
        } else {
            d.children = d._children;
            d._children = null;
        }
        update(d);
    }

    // Breadth-first traversal of the tree
    // func function is processed on every node of a same level
    // return the max level
    function breadthFirstTraversal(tree: any, func: any)
    {
        let max = 0;
        if (tree && tree.length > 0)
        {
            let currentDepth = tree[0].depth;
            const fifo: any = [];
            let currentLevel: any = [];

            fifo.push(tree[0]);
            while (fifo.length > 0) {
                const node: any = fifo.shift();
                if (node.depth > currentDepth) {
                    func(currentLevel);
                    currentDepth++;
                    max = Math.max(max, currentLevel.length);
                    currentLevel = [];
                }
                currentLevel.push(node);
                if (node.children) {
                    for (let j = 0; j < node.children.length; j++) {
                        fifo.push(node.children[j]);
                    }
                }
            }
            func(currentLevel);
            return Math.max(max, currentLevel.length);
        }
        return 0;
    }

    // x = ordoninates and y = abscissas
    function collision(siblings) {
        const minPadding = 5;
        if (siblings) {
            for (let i = 0; i < siblings.length - 1; i++)
            {
                if (siblings[i + 1].x - (siblings[i].x + rectNode.height) < minPadding)
                    siblings[i + 1].x = siblings[i].x + rectNode.height + minPadding;
            }
        }
    }

    function removeMouseEvents() {
        // Drag and zoom behaviors are temporarily disabled, so tooltip text can be selected
        mousedown = d3.select(selector).select('svg').on('mousedown.zoom');
        d3.select(selector).select('svg').on("mousedown.zoom", null);
    }

    function reactivateMouseEvents() {
        // Reactivate the drag and zoom behaviors
        d3.select(selector).select('svg').on('mousedown.zoom', mousedown);
    }

    // Name of the event depends of the browser
    function getMouseWheelEvent() {
        if (d3.select(selector).select('svg').on('wheel.zoom'))
        {
            mouseWheelName = 'wheel.zoom';
            return d3.select(selector).select('svg').on('wheel.zoom');
        }
        if (d3.select(selector).select('svg').on('mousewheel.zoom') != null)
        {
            mouseWheelName = 'mousewheel.zoom';
            return d3.select(selector).select('svg').on('mousewheel.zoom');
        }
        if (d3.select(selector).select('svg').on('DOMMouseScroll.zoom'))
        {
            mouseWheelName = 'DOMMouseScroll.zoom';
            return d3.select(selector).select('svg').on('DOMMouseScroll.zoom');
        }
    }

    function diagonal(d) {
        const p0 = {
            x : d.source.x + rectNode.height / 2,
            y : (d.source.y + rectNode.width)
        },
        p3 = {
            x : d.target.x + rectNode.height / 2,
            y : d.target.y  - 12 // -12, so the end arrows are just before the rect node
        },
        m = (p0.y + p3.y) / 2;

        let p: any = [
            p0, {
                x : p0.x,
                y : m
            }, {
                x : p3.x,
                y : m
            }, p3
        ];
        p = p.map(function(d) {
            return [ d.y, d.x ];
        });
        return 'M' + p[0] + 'C' + p[1] + ' ' + p[2] + ' ' + p[3];
    }

    function initDropShadow() {
        const filter = defs.append("filter")
            .attr("id", "drop-shadow")
            .attr("color-interpolation-filters", "sRGB");

        filter.append("feOffset")
            .attr("result", "offOut")
            .attr("in", "SourceGraphic")
            .attr("dx", 0)
            .attr("dy", 0);

        filter.append("feGaussianBlur")
            .attr("stdDeviation", 2);

        filter.append("feOffset")
            .attr("dx", 2)
            .attr("dy", 2)
            .attr("result", "shadow");

        filter.append("feComposite")
            .attr("in", 'offOut')
            .attr("in2", 'shadow')
            .attr("operator", "over");
    }

    function initArrowDef() {
        // Build the arrows definitions
        // End arrow
        defs.append('marker')
            .attr('id', 'end-arrow')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 0)
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .attr('class', 'arrow')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5');

        // End arrow selected
        defs.append('marker')
            .attr('id', 'end-arrow-selected')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 0)
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .attr('class', 'arrowselected')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5');

        // Start arrow
        defs.append('marker')
            .attr('id', 'start-arrow')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 0)
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .attr('class', 'arrow')
            .append('path')
            .attr('d', 'M10,-5L0,0L10,5');

        // Start arrow selected
        defs.append('marker')
            .attr('id', 'start-arrow-selected')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 0)
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .attr('class', 'arrowselected')
            .append('path')
            .attr('d', 'M10,-5L0,0L10,5');
    }
}

export { treeBoxes };
