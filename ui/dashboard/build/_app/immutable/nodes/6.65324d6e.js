import { S as SvelteComponent, i as init, s as safe_not_equal, k as element, l as claim_element, m as children, h as detach, n as attr, b as insert_hydration, J as noop, K as component_subscribe, o as onMount, q as text, a as space, r as claim_text, c as claim_space, F as append_hydration, u as set_data } from "../chunks/index.56839d90.js";
import { w as writable } from "../chunks/index.7b465657.js";
const error = writable("");
const blocks = writable([
  {
    "hash": "20a0af030c51d8f13313bdcd5f7ba2dd362683b080f3b3d54e8eee44f6d7ecf8",
    "previousblockhash": "202f911177c040aa7261e3ed3780b12f44378f2951f49878c6caa432cece228c",
    "height": 72431
  },
  {
    "hash": "202f911177c040aa7261e3ed3780b12f44378f2951f49878c6caa432cece228c",
    "previousblockhash": "10e32fd9f6f014198c3447400a796c53c1b8077abbc1613a51541967f9350297",
    "height": 72430
  },
  {
    "hash": "10a0af030c51d8f13313bdcd5f7ba2dd362683b080f3b3d54e8eee44f6d7ecf8",
    "previousblockhash": "102f911177c040aa7261e3ed3780b12f44378f2951f49878c6caa432cece228c",
    "height": 72431
  },
  {
    "hash": "102f911177c040aa7261e3ed3780b12f44378f2951f49878c6caa432cece228c",
    "previousblockhash": "10e32fd9f6f014198c3447400a796c53c1b8077abbc1613a51541967f9350297",
    "height": 72430
  },
  {
    "hash": "10e32fd9f6f014198c3447400a796c53c1b8077abbc1613a51541967f9350297",
    "previousblockhash": "10457e571ac6540c2756115b45b26cb328966c2aa9dc434b2655229d9a17e39a",
    "height": 72429
  },
  {
    "hash": "10457e571ac6540c2756115b45b26cb328966c2aa9dc434b2655229d9a17e39a",
    "previousblockhash": "00a036054e965cb34640aab69d448268a1c0b93c7bf3d55cc1d7dbc7c20f7cef",
    "height": 72428
  },
  {
    "hash": "00a0af030c51d8f13313bdcd5f7ba2dd362683b080f3b3d54e8eee44f6d7ecf8",
    "previousblockhash": "002f911177c040aa7261e3ed3780b12f44378f2951f49878c6caa432cece228c",
    "height": 72431
  },
  {
    "hash": "002f911177c040aa7261e3ed3780b12f44378f2951f49878c6caa432cece228c",
    "previousblockhash": "00e32fd9f6f014198c3447400a796c53c1b8077abbc1613a51541967f9350297",
    "height": 72430
  },
  {
    "hash": "00e32fd9f6f014198c3447400a796c53c1b8077abbc1613a51541967f9350297",
    "previousblockhash": "00457e571ac6540c2756115b45b26cb328966c2aa9dc434b2655229d9a17e39a",
    "height": 72429
  },
  {
    "hash": "00457e571ac6540c2756115b45b26cb328966c2aa9dc434b2655229d9a17e39a",
    "previousblockhash": "00a036054e965cb34640aab69d448268a1c0b93c7bf3d55cc1d7dbc7c20f7cef",
    "height": 72428
  },
  {
    "hash": "00a036054e965cb34640aab69d448268a1c0b93c7bf3d55cc1d7dbc7c20f7cef",
    "previousblockhash": "0023074edfca79d133e0f896bcb959f18ee8a25b52b0c06c1f43a17a94e34f40",
    "height": 72427
  },
  {
    "hash": "0023074edfca79d133e0f896bcb959f18ee8a25b52b0c06c1f43a17a94e34f40",
    "previousblockhash": "00e0f11082deb76ee9d2db699907434fefbf1fd251e70037efa5cd01fc0d9884",
    "height": 72426
  },
  {
    "hash": "00e0f11082deb76ee9d2db699907434fefbf1fd251e70037efa5cd01fc0d9884",
    "previousblockhash": "00182f7661239b21458f109895e3e792db844736580d6839b39f83ebe646e5d5",
    "height": 72425
  },
  {
    "hash": "00182f7661239b21458f109895e3e792db844736580d6839b39f83ebe646e5d5",
    "previousblockhash": "00cf344ff0f814452be44941d3f2ddb12bfa664b5ba184ef53e92803bd6693a4",
    "height": 72424
  },
  {
    "hash": "00cf344ff0f814452be44941d3f2ddb12bfa664b5ba184ef53e92803bd6693a4",
    "previousblockhash": "00de7ab5a797c5d8d720ce79c1be7f01aa8aa3c03d537e1a11a05bc08416aba7",
    "height": 72423
  },
  {
    "hash": "00de7ab5a797c5d8d720ce79c1be7f01aa8aa3c03d537e1a11a05bc08416aba7",
    "previousblockhash": "008e5cc610d607c8c8874c3a35e274a8c848a0e4c78b805352c3dab7fb4bd2a0",
    "height": 72422
  }
]);
const _page_svelte_svelte_type_style_lang = "";
function create_else_block(ctx) {
  let section;
  let div0;
  let t0;
  let t1;
  let div1;
  let t2;
  let pre;
  let t3_value = JSON.stringify(
    /*unique*/
    ctx[0],
    null,
    2
  ) + "";
  let t3;
  return {
    c() {
      section = element("section");
      div0 = element("div");
      t0 = text("Last 10 blocks from demo data");
      t1 = space();
      div1 = element("div");
      t2 = space();
      pre = element("pre");
      t3 = text(t3_value);
      this.h();
    },
    l(nodes) {
      section = claim_element(nodes, "SECTION", { class: true });
      var section_nodes = children(section);
      div0 = claim_element(section_nodes, "DIV", { class: true });
      var div0_nodes = children(div0);
      t0 = claim_text(div0_nodes, "Last 10 blocks from demo data");
      div0_nodes.forEach(detach);
      t1 = claim_space(section_nodes);
      div1 = claim_element(section_nodes, "DIV", { class: true, id: true });
      children(div1).forEach(detach);
      t2 = claim_space(section_nodes);
      pre = claim_element(section_nodes, "PRE", {});
      var pre_nodes = children(pre);
      t3 = claim_text(pre_nodes, t3_value);
      pre_nodes.forEach(detach);
      section_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(div0, "class", "field");
      attr(div1, "class", "full svelte-1x7h1fw");
      attr(div1, "id", "tree");
      attr(section, "class", "section");
    },
    m(target, anchor) {
      insert_hydration(target, section, anchor);
      append_hydration(section, div0);
      append_hydration(div0, t0);
      append_hydration(section, t1);
      append_hydration(section, div1);
      append_hydration(section, t2);
      append_hydration(section, pre);
      append_hydration(pre, t3);
    },
    p(ctx2, dirty) {
      if (dirty & /*unique*/
      1 && t3_value !== (t3_value = JSON.stringify(
        /*unique*/
        ctx2[0],
        null,
        2
      ) + ""))
        set_data(t3, t3_value);
    },
    d(detaching) {
      if (detaching)
        detach(section);
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
        ctx[1]
      );
    },
    l(nodes) {
      p = claim_element(nodes, "P", {});
      var p_nodes = children(p);
      t = claim_text(
        p_nodes,
        /*$error*/
        ctx[1]
      );
      p_nodes.forEach(detach);
    },
    m(target, anchor) {
      insert_hydration(target, p, anchor);
      append_hydration(p, t);
    },
    p(ctx2, dirty) {
      if (dirty & /*$error*/
      2)
        set_data(
          t,
          /*$error*/
          ctx2[1]
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
      ctx2[1]
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
      this.h();
    },
    l(nodes) {
      div = claim_element(nodes, "DIV", { class: true });
      var div_nodes = children(div);
      if_block.l(div_nodes);
      div_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(div, "class", "full svelte-1x7h1fw");
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
  let $error;
  component_subscribe($$self, blocks, ($$value) => $$invalidate(2, $blocks = $$value));
  component_subscribe($$self, error, ($$value) => $$invalidate(1, $error = $$value));
  let unique = [];
  onMount(() => {
    const width = 600;
    const height = 400;
    const svg = d3.select("#tree").append("svg").attr("width", "100%").attr("height", "100vh");
    $$invalidate(0, unique = getUniqueValues($blocks));
    const treeData = createHierarchy(unique);
    const root = d3.hierarchy(treeData);
    const treeLayout = d3.tree().size([height - 100, width]).separation((a, b) => {
      return a.parent == b.parent ? 2 : 3;
    });
    treeLayout(root);
    const g = svg.append("g").attr("transform", "translate(100,0)");
    g.selectAll(".link").data(root.descendants().slice(1)).enter().append("path").attr("class", "link").attr("d", (d) => {
      return "M" + d.y + "," + d.x + "C" + (d.y + d.parent.y) / 2 + "," + d.x + " " + (d.y + d.parent.y) / 2 + "," + d.parent.x + " " + d.parent.y + "," + d.parent.x;
    });
    const node = g.selectAll(".node").data(root.descendants()).enter().append("g").attr("class", (d) => "node" + (d.children ? " node--internal" : " node--leaf")).attr("transform", (d) => "translate(" + d.y + "," + d.x + ")");
    node.append("rect").attr("width", 40).attr("height", 35).attr("x", -30).attr("y", -15).append("title").text((d) => d.data.name);
    node.append("text").attr("dy", -5).attr("dx", -10).style("text-anchor", "middle").text((d) => d.data.name.substring(0, 6));
    node.append("text").attr("dy", 15).attr("dx", -10).style("text-anchor", "middle").text((d) => d.data.height);
  });
  return [unique, $error];
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
//# sourceMappingURL=6.65324d6e.js.map
