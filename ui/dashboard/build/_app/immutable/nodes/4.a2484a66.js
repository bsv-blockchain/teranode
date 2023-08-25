import { S as SvelteComponent, i as init, s as safe_not_equal, k as element, l as claim_element, m as children, h as detach, b as insert_hydration, I as noop, J as component_subscribe, o as onMount, n as attr, q as text, r as claim_text, E as append_hydration, u as set_data } from "../chunks/index.0770bd3b.js";
import { b as blocks, e as error } from "../chunks/nodeStore.e8e10a91.js";
const _page_svelte_svelte_type_style_lang = "";
function create_else_block(ctx) {
  let div;
  return {
    c() {
      div = element("div");
      this.h();
    },
    l(nodes) {
      div = claim_element(nodes, "DIV", { id: true });
      children(div).forEach(detach);
      this.h();
    },
    h() {
      attr(div, "id", "tree");
    },
    m(target, anchor) {
      insert_hydration(target, div, anchor);
    },
    p: noop,
    d(detaching) {
      if (detaching)
        detach(div);
    }
  };
}
function create_if_block(ctx) {
  let p;
  let t;
  return {
    c() {
      p = element("p");
      t = text(
        /*$error*/
        ctx[0]
      );
    },
    l(nodes) {
      p = claim_element(nodes, "P", {});
      var p_nodes = children(p);
      t = claim_text(
        p_nodes,
        /*$error*/
        ctx[0]
      );
      p_nodes.forEach(detach);
    },
    m(target, anchor) {
      insert_hydration(target, p, anchor);
      append_hydration(p, t);
    },
    p(ctx2, dirty) {
      if (dirty & /*$error*/
      1)
        set_data(
          t,
          /*$error*/
          ctx2[0]
        );
    },
    d(detaching) {
      if (detaching)
        detach(p);
    }
  };
}
function create_fragment(ctx) {
  let div;
  function select_block_type(ctx2, dirty) {
    if (
      /*$error*/
      ctx2[0]
    )
      return create_if_block;
    return create_else_block;
  }
  let current_block_type = select_block_type(ctx);
  let if_block = current_block_type(ctx);
  return {
    c() {
      div = element("div");
      if_block.c();
    },
    l(nodes) {
      div = claim_element(nodes, "DIV", {});
      var div_nodes = children(div);
      if_block.l(div_nodes);
      div_nodes.forEach(detach);
    },
    m(target, anchor) {
      insert_hydration(target, div, anchor);
      if_block.m(div, null);
    },
    p(ctx2, [dirty]) {
      if (current_block_type === (current_block_type = select_block_type(ctx2)) && if_block) {
        if_block.p(ctx2, dirty);
      } else {
        if_block.d(1);
        if_block = current_block_type(ctx2);
        if (if_block) {
          if_block.c();
          if_block.m(div, null);
        }
      }
    },
    i: noop,
    o: noop,
    d(detaching) {
      if (detaching)
        detach(div);
      if_block.d();
    }
  };
}
function getUniqueValues(obj) {
  let values = [];
  for (let key in obj) {
    values = values.concat(obj[key]);
  }
  return [...new Set(values)];
}
function createHierarchy(arr) {
  const nodeMap = {};
  let rootNode = null;
  arr.forEach((item) => {
    if (!nodeMap[item.hash]) {
      nodeMap[item.hash] = { name: item.hash, children: [] };
    }
    const currentNode = nodeMap[item.hash];
    if (!nodeMap[item.previousblockhash]) {
      nodeMap[item.previousblockhash] = {
        name: item.previousblockhash,
        children: []
      };
    }
    const parentNode = nodeMap[item.previousblockhash];
    parentNode.children.push(currentNode);
    if (!arr.some((i) => i.hash === item.previousblockhash)) {
      rootNode = parentNode;
    }
  });
  return {
    name: "Root",
    children: rootNode ? [rootNode] : []
  };
}
function instance($$self, $$props, $$invalidate) {
  let $blocks;
  let $error;
  component_subscribe($$self, blocks, ($$value) => $$invalidate(1, $blocks = $$value));
  component_subscribe($$self, error, ($$value) => $$invalidate(0, $error = $$value));
  onMount(() => {
    const width = 600;
    const height = 400;
    const svg = d3.select("#tree").append("svg").attr("width", width).attr("height", height);
    const treeData = createHierarchy(getUniqueValues($blocks));
    debugger;
    const root = d3.hierarchy(treeData);
    const treeLayout = d3.tree().size([height, width - 6e3]);
    treeLayout(root);
    const g = svg.append("g").attr("transform", "translate(1000,0)");
    g.selectAll(".link").data(root.descendants().slice(1)).enter().append("path").attr("class", "link").attr("d", (d) => {
      return "M" + d.y + "," + d.x + "C" + (d.y + d.parent.y) / 2 + "," + d.x + " " + (d.y + d.parent.y) / 2 + "," + d.parent.x + " " + d.parent.y + "," + d.parent.x;
    });
    const node = g.selectAll(".node").data(root.descendants()).enter().append("g").attr("class", (d) => "node" + (d.children ? " node--internal" : " node--leaf")).attr("transform", (d) => "translate(" + d.y + "," + d.x + ")");
    node.append("circle").attr("r", 10);
    node.append("text").attr("dy", 3).attr("x", (d) => d.children ? -15 : 15).style("text-anchor", (d) => d.children ? "end" : "start").text((d) => d.data.name.substring(0, 6));
  });
  return [$error];
}
class Page extends SvelteComponent {
  constructor(options) {
    super();
    init(this, options, instance, create_fragment, safe_not_equal, {});
  }
}
export {
  Page as component
};
//# sourceMappingURL=4.a2484a66.js.map
