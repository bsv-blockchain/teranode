<script lang="ts">
    import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
    import {onMount} from "svelte";

    import {treeBoxes} from "./helpers";
    import * as api from "$internal/api";
    import {page} from "$app/stores";

    let vis; // binding with div for visualization
    let tree; // tree data
    let pageSize = 20;

    $: hash = $page.url.searchParams.get('hash') || '';

    //let hash = "0048b884a7098dc33b7ef4a7ff1cd22fce98de9e0d0801f247351df947f97c21"

    async function getStatsData() {
        try {
            const result: any = await api.getBlockForks({
                hash,
                limit: pageSize,
            })
            if (result.ok) {
                tree = result.data.tree;
                redraw();
            }
        } catch (e) {
            console.error(e)
        }
    }

    function redraw() {
        console.log(tree)
        treeBoxes(vis, tree);
    }

    onMount(() => {
        getStatsData();
        window.addEventListener('resize', redraw);
    })
</script>

<PageWithMenu>
    <div class="content">
        <div id="vis" bind:this={vis}></div>
    </div>
</PageWithMenu>

<style>
    .content {
        width: 100%;
        max-width: 100%;
        overflow: scroll;

        display: flex;
        flex-direction: column;
        gap: 20px;
    }

    #vis {
        position: absolute;
        left: 0;
        width: 100%;
        overflow: scroll;

        background-color: #aaa;
        padding: 20px;
    }

    :global(.svgContainer) {
        display: block;
        margin: auto;
    }

    :global(.node) {
        cursor: pointer;
    }

    :global(.node-rect) {
    }

    :global(.node-rect-closed) {
        stroke-width: 2px;
        stroke: rgb(0, 0, 0);
    }

    :global(.link) {
        fill: none;
        stroke: lightsteelblue;
        stroke-width: 2px;
    }

    :global(.linkselected) {
        fill: none;
        stroke: tomato;
        stroke-width: 2px;
    }

    :global(.arrow) {
        fill: lightsteelblue;
        stroke-width: 1px;
    }

    :global(.arrowselected) {
        fill: tomato;
        stroke-width: 2px;
    }

    :global(.link text) {
        font: 7px sans-serif;
        fill: #CC0000;
    }

    :global(.wordwrap) {
        white-space: pre-wrap; /* CSS3 */
        white-space: -moz-pre-wrap; /* Firefox */
        white-space: -pre-wrap; /* Opera <7 */
        white-space: -o-pre-wrap; /* Opera 7 */
        word-wrap: break-word; /* IE */
    }

    :global(.node-text) {
        font: 10px sans-serif;
        font-weight: normal;
        color: white;
    }

    :global(.tooltip-text-container) {
        height: 100%;
        width: 100%;
    }

    :global(.tooltip-text) {
        visibility: hidden;
        font: 7px sans-serif;
        color: white;
        display: block;
        padding: 5px;
    }

    :global(.tooltip-box) {
        background: rgba(0, 0, 0, 0.7);
        visibility: hidden;
        position: absolute;
        border-style: solid;
        border-width: 1px;
        border-color: black;
        border-top-right-radius: 0.5em;
    }

    :global(.textcolored) {
        color: orange;
    }

    :global(a.exchangeName) {
        color: orange;
    }
</style>
