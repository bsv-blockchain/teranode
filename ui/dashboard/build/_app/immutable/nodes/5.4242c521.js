import { S as SvelteComponent, i as init, s as safe_not_equal, k as element, a as space, q as text, l as claim_element, m as children, c as claim_space, r as claim_text, h as detach, n as attr, b as insert_hydration, F as append_hydration, g as transition_in, d as transition_out, f as check_outros, u as set_data, K as component_subscribe, y as create_component, z as claim_component, A as mount_component, B as destroy_component, v as group_outros } from "../chunks/index.ca9f8ef3.js";
import { b as blocks, n as nodes, l as loading } from "../chunks/nodeStore.f7b61ab1.js";
import { S as Spinner } from "../chunks/Spinner.144c4515.js";
const _page_svelte_svelte_type_style_lang = "";
function create_if_block(ctx) {
  let spinner;
  let current;
  spinner = new Spinner({});
  return {
    c() {
      create_component(spinner.$$.fragment);
    },
    l(nodes2) {
      claim_component(spinner.$$.fragment, nodes2);
    },
    m(target, anchor) {
      mount_component(spinner, target, anchor);
      current = true;
    },
    i(local) {
      if (current)
        return;
      transition_in(spinner.$$.fragment, local);
      current = true;
    },
    o(local) {
      transition_out(spinner.$$.fragment, local);
      current = false;
    },
    d(detaching) {
      destroy_component(spinner, detaching);
    }
  };
}
function create_fragment(ctx) {
  let div3;
  let t0;
  let section;
  let div0;
  let t1;
  let t2_value = (
    /*$nodes*/
    ctx[2].length + ""
  );
  let t2;
  let t3;
  let t4;
  let div2;
  let div1;
  let t5;
  let pre;
  let t6_value = JSON.stringify(
    /*unique*/
    ctx[0],
    null,
    2
  ) + "";
  let t6;
  let current;
  let if_block = (
    /*localLoading*/
    (ctx[1] || /*$nodes*/
    ctx[2].length === 0 && /*$loading*/
    ctx[3]) && create_if_block()
  );
  return {
    c() {
      div3 = element("div");
      if (if_block)
        if_block.c();
      t0 = space();
      section = element("section");
      div0 = element("div");
      t1 = text("Last 10 blocks from ");
      t2 = text(t2_value);
      t3 = text(" connected nodes");
      t4 = space();
      div2 = element("div");
      div1 = element("div");
      t5 = space();
      pre = element("pre");
      t6 = text(t6_value);
      this.h();
    },
    l(nodes2) {
      div3 = claim_element(nodes2, "DIV", { class: true });
      var div3_nodes = children(div3);
      if (if_block)
        if_block.l(div3_nodes);
      t0 = claim_space(div3_nodes);
      section = claim_element(div3_nodes, "SECTION", { class: true });
      var section_nodes = children(section);
      div0 = claim_element(section_nodes, "DIV", { class: true });
      var div0_nodes = children(div0);
      t1 = claim_text(div0_nodes, "Last 10 blocks from ");
      t2 = claim_text(div0_nodes, t2_value);
      t3 = claim_text(div0_nodes, " connected nodes");
      div0_nodes.forEach(detach);
      t4 = claim_space(section_nodes);
      div2 = claim_element(section_nodes, "DIV", { class: true });
      var div2_nodes = children(div2);
      div1 = claim_element(div2_nodes, "DIV", { id: true });
      children(div1).forEach(detach);
      div2_nodes.forEach(detach);
      t5 = claim_space(section_nodes);
      pre = claim_element(section_nodes, "PRE", {});
      var pre_nodes = children(pre);
      t6 = claim_text(pre_nodes, t6_value);
      pre_nodes.forEach(detach);
      section_nodes.forEach(detach);
      div3_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(div0, "class", "field");
      attr(div1, "id", "tree");
      attr(div2, "class", "full svelte-enjiyv");
      attr(section, "class", "section");
      attr(div3, "class", "full svelte-enjiyv");
    },
    m(target, anchor) {
      insert_hydration(target, div3, anchor);
      if (if_block)
        if_block.m(div3, null);
      append_hydration(div3, t0);
      append_hydration(div3, section);
      append_hydration(section, div0);
      append_hydration(div0, t1);
      append_hydration(div0, t2);
      append_hydration(div0, t3);
      append_hydration(section, t4);
      append_hydration(section, div2);
      append_hydration(div2, div1);
      append_hydration(section, t5);
      append_hydration(section, pre);
      append_hydration(pre, t6);
      current = true;
    },
    p(ctx2, [dirty]) {
      if (
        /*localLoading*/
        ctx2[1] || /*$nodes*/
        ctx2[2].length === 0 && /*$loading*/
        ctx2[3]
      ) {
        if (if_block) {
          if (dirty & /*localLoading, $nodes, $loading*/
          14) {
            transition_in(if_block, 1);
          }
        } else {
          if_block = create_if_block();
          if_block.c();
          transition_in(if_block, 1);
          if_block.m(div3, t0);
        }
      } else if (if_block) {
        group_outros();
        transition_out(if_block, 1, 1, () => {
          if_block = null;
        });
        check_outros();
      }
      if ((!current || dirty & /*$nodes*/
      4) && t2_value !== (t2_value = /*$nodes*/
      ctx2[2].length + ""))
        set_data(t2, t2_value);
      if ((!current || dirty & /*unique*/
      1) && t6_value !== (t6_value = JSON.stringify(
        /*unique*/
        ctx2[0],
        null,
        2
      ) + ""))
        set_data(t6, t6_value);
    },
    i(local) {
      if (current)
        return;
      transition_in(if_block);
      current = true;
    },
    o(local) {
      transition_out(if_block);
      current = false;
    },
    d(detaching) {
      if (detaching)
        detach(div3);
      if (if_block)
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
  arr.forEach((item) => {
    if (!nodeMap[item.hash]) {
      nodeMap[item.hash] = {
        name: item.hash,
        height: item.height,
        children: []
      };
    }
    const currentNode = nodeMap[item.hash];
    if (!item.previousblockhash) {
      return;
    }
    if (!nodeMap[item.previousblockhash]) {
      nodeMap[item.previousblockhash] = {
        name: item.previousblockhash,
        height: item.height - 1,
        // Assuming the height of parent is always current height - 1
        children: []
      };
    }
    const parentNode = nodeMap[item.previousblockhash];
    parentNode.children.push(currentNode);
  });
  const rootNode = Object.values(nodeMap).find((node) => !arr.some((item) => item.hash === node.name));
  return rootNode || {};
}
function instance($$self, $$props, $$invalidate) {
  let $blocks;
  let $nodes;
  let $loading;
  component_subscribe($$self, blocks, ($$value) => $$invalidate(4, $blocks = $$value));
  component_subscribe($$self, nodes, ($$value) => $$invalidate(2, $nodes = $$value));
  component_subscribe($$self, loading, ($$value) => $$invalidate(3, $loading = $$value));
  let localLoading = false;
  let unique = [];
  $$self.$$.update = () => {
    if ($$self.$$.dirty & /*$blocks, unique*/
    17) {
      if (Object.keys($blocks).length > 0) {
        try {
          const width = 600;
          const height = 400;
          $$invalidate(1, localLoading = true);
          $$invalidate(0, unique = getUniqueValues($blocks));
          const treeData = createHierarchy(unique);
          d3.select("#tree").select("svg").remove();
          const svg = d3.select("#tree").append("svg").attr("width", "100%").attr("height", "100%");
          const root = d3.hierarchy(treeData);
          const treeLayout = d3.tree().size([height - 200, width]);
          treeLayout(root);
          const g = svg.append("g").attr("transform", "translate(100,0)");
          const link = g.selectAll(".link").data(root.descendants().slice(1)).enter().append("path").attr("class", "link").attr("d", (d) => {
            return "M" + d.y + "," + d.x + "C" + (d.y + d.parent.y) / 2 + "," + d.x + " " + (d.y + d.parent.y) / 2 + "," + d.parent.x + " " + d.parent.y + "," + d.parent.x;
          });
          const node = g.selectAll(".node").data(root.descendants()).enter().append("g").attr("class", (d) => "node" + (d.children ? " node--internal" : " node--leaf")).attr("transform", (d) => "translate(" + d.y + "," + d.x + ")");
          node.append("circle").attr("r", 10).append("title").text((d) => d.data.name);
          node.append("text").attr("dy", 30).attr("x", -15).style("text-anchor", (d) => d.children ? "start" : "start").text((d) => d.data.height);
        } catch (err) {
          console.log(err);
        } finally {
          $$invalidate(1, localLoading = false);
        }
      }
    }
  };
  return [unique, localLoading, $nodes, $loading, $blocks];
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
//# sourceMappingURL=5.4242c521.js.map
