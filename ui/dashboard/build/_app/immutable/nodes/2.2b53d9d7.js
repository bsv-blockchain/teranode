import { S as SvelteComponent, i as init, s as safe_not_equal, k as element, q as text, a as space, l as claim_element, m as children, r as claim_text, h as detach, c as claim_space, n as attr, b as insert_hydration, E as append_hydration, I as noop, J as component_subscribe, K as update_keyed_each, u as set_data, L as destroy_block, v as group_outros, d as transition_out, f as check_outros, g as transition_in, e as empty, M as outro_and_destroy_block, y as create_component, z as claim_component, A as mount_component, B as destroy_component } from "../chunks/index.0770bd3b.js";
import { e as error, n as nodes } from "../chunks/nodeStore.e8e10a91.js";
const ConnectedNodes_svelte_svelte_type_style_lang = "";
function get_each_context$1(ctx, list, i) {
  const child_ctx = ctx.slice();
  child_ctx[2] = list[i];
  return child_ctx;
}
function create_else_block$1(ctx) {
  let table;
  let tbody;
  let each_blocks = [];
  let each_1_lookup = /* @__PURE__ */ new Map();
  let each_value = (
    /*$nodes*/
    ctx[1]
  );
  const get_key = (ctx2) => (
    /*node*/
    ctx2[2].ip
  );
  for (let i = 0; i < each_value.length; i += 1) {
    let child_ctx = get_each_context$1(ctx, each_value, i);
    let key = get_key(child_ctx);
    each_1_lookup.set(key, each_blocks[i] = create_each_block$1(key, child_ctx));
  }
  return {
    c() {
      table = element("table");
      tbody = element("tbody");
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].c();
      }
      this.h();
    },
    l(nodes2) {
      table = claim_element(nodes2, "TABLE", { class: true });
      var table_nodes = children(table);
      tbody = claim_element(table_nodes, "TBODY", {});
      var tbody_nodes = children(tbody);
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].l(tbody_nodes);
      }
      tbody_nodes.forEach(detach);
      table_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(table, "class", "table svelte-1spn3tb");
    },
    m(target, anchor) {
      insert_hydration(target, table, anchor);
      append_hydration(table, tbody);
      for (let i = 0; i < each_blocks.length; i += 1) {
        if (each_blocks[i]) {
          each_blocks[i].m(tbody, null);
        }
      }
    },
    p(ctx2, dirty) {
      if (dirty & /*humanTime, $nodes, Date*/
      2) {
        each_value = /*$nodes*/
        ctx2[1];
        each_blocks = update_keyed_each(each_blocks, dirty, get_key, 1, ctx2, each_value, each_1_lookup, tbody, destroy_block, create_each_block$1, null, get_each_context$1);
      }
    },
    d(detaching) {
      if (detaching)
        detach(table);
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].d();
      }
    }
  };
}
function create_if_block_1$1(ctx) {
  let p;
  let t;
  return {
    c() {
      p = element("p");
      t = text("No nodes connected");
    },
    l(nodes2) {
      p = claim_element(nodes2, "P", {});
      var p_nodes = children(p);
      t = claim_text(p_nodes, "No nodes connected");
      p_nodes.forEach(detach);
    },
    m(target, anchor) {
      insert_hydration(target, p, anchor);
      append_hydration(p, t);
    },
    p: noop,
    d(detaching) {
      if (detaching)
        detach(p);
    }
  };
}
function create_if_block$1(ctx) {
  let p;
  let t0;
  let t1;
  return {
    c() {
      p = element("p");
      t0 = text("Error fetching nodes: ");
      t1 = text(
        /*$error*/
        ctx[0]
      );
    },
    l(nodes2) {
      p = claim_element(nodes2, "P", {});
      var p_nodes = children(p);
      t0 = claim_text(p_nodes, "Error fetching nodes: ");
      t1 = claim_text(
        p_nodes,
        /*$error*/
        ctx[0]
      );
      p_nodes.forEach(detach);
    },
    m(target, anchor) {
      insert_hydration(target, p, anchor);
      append_hydration(p, t0);
      append_hydration(p, t1);
    },
    p(ctx2, dirty) {
      if (dirty & /*$error*/
      1)
        set_data(
          t1,
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
function create_each_block$1(key_1, ctx) {
  let tr;
  let td0;
  let t0_value = (
    /*node*/
    (ctx[2].blobServerHTTPAddress || "anonymous") + ""
  );
  let t0;
  let t1;
  let td1;
  let t2_value = (
    /*node*/
    ctx[2].source + ""
  );
  let t2;
  let t3;
  let td2;
  let t4_value = (
    /*node*/
    ctx[2].name + ""
  );
  let t4;
  let t5;
  let td3;
  let t6_value = (
    /*node*/
    ctx[2].ip + ""
  );
  let t6;
  let t7;
  let td4;
  let t8;
  let t9_value = new Date(
    /*node*/
    ctx[2].connectedAt
  ).toISOString().replace("T", " ") + "";
  let t9;
  let t10;
  let br;
  let t11;
  let span;
  let t12_value = humanTime(
    /*node*/
    ctx[2].connectedAt
  ) + "";
  let t12;
  let t13;
  let t14;
  return {
    key: key_1,
    first: null,
    c() {
      tr = element("tr");
      td0 = element("td");
      t0 = text(t0_value);
      t1 = space();
      td1 = element("td");
      t2 = text(t2_value);
      t3 = space();
      td2 = element("td");
      t4 = text(t4_value);
      t5 = space();
      td3 = element("td");
      t6 = text(t6_value);
      t7 = space();
      td4 = element("td");
      t8 = text("Connected at ");
      t9 = text(t9_value);
      t10 = space();
      br = element("br");
      t11 = space();
      span = element("span");
      t12 = text(t12_value);
      t13 = text(" ago");
      t14 = space();
      this.h();
    },
    l(nodes2) {
      tr = claim_element(nodes2, "TR", { class: true });
      var tr_nodes = children(tr);
      td0 = claim_element(tr_nodes, "TD", { class: true });
      var td0_nodes = children(td0);
      t0 = claim_text(td0_nodes, t0_value);
      td0_nodes.forEach(detach);
      t1 = claim_space(tr_nodes);
      td1 = claim_element(tr_nodes, "TD", { class: true });
      var td1_nodes = children(td1);
      t2 = claim_text(td1_nodes, t2_value);
      td1_nodes.forEach(detach);
      t3 = claim_space(tr_nodes);
      td2 = claim_element(tr_nodes, "TD", { class: true });
      var td2_nodes = children(td2);
      t4 = claim_text(td2_nodes, t4_value);
      td2_nodes.forEach(detach);
      t5 = claim_space(tr_nodes);
      td3 = claim_element(tr_nodes, "TD", { class: true });
      var td3_nodes = children(td3);
      t6 = claim_text(td3_nodes, t6_value);
      td3_nodes.forEach(detach);
      t7 = claim_space(tr_nodes);
      td4 = claim_element(tr_nodes, "TD", { class: true });
      var td4_nodes = children(td4);
      t8 = claim_text(td4_nodes, "Connected at ");
      t9 = claim_text(td4_nodes, t9_value);
      t10 = claim_space(td4_nodes);
      br = claim_element(td4_nodes, "BR", {});
      t11 = claim_space(td4_nodes);
      span = claim_element(td4_nodes, "SPAN", { class: true });
      var span_nodes = children(span);
      t12 = claim_text(span_nodes, t12_value);
      t13 = claim_text(span_nodes, " ago");
      span_nodes.forEach(detach);
      td4_nodes.forEach(detach);
      t14 = claim_space(tr_nodes);
      tr_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(td0, "class", "svelte-1spn3tb");
      attr(td1, "class", "svelte-1spn3tb");
      attr(td2, "class", "svelte-1spn3tb");
      attr(td3, "class", "svelte-1spn3tb");
      attr(span, "class", "small svelte-1spn3tb");
      attr(td4, "class", "svelte-1spn3tb");
      attr(tr, "class", "svelte-1spn3tb");
      this.first = tr;
    },
    m(target, anchor) {
      insert_hydration(target, tr, anchor);
      append_hydration(tr, td0);
      append_hydration(td0, t0);
      append_hydration(tr, t1);
      append_hydration(tr, td1);
      append_hydration(td1, t2);
      append_hydration(tr, t3);
      append_hydration(tr, td2);
      append_hydration(td2, t4);
      append_hydration(tr, t5);
      append_hydration(tr, td3);
      append_hydration(td3, t6);
      append_hydration(tr, t7);
      append_hydration(tr, td4);
      append_hydration(td4, t8);
      append_hydration(td4, t9);
      append_hydration(td4, t10);
      append_hydration(td4, br);
      append_hydration(td4, t11);
      append_hydration(td4, span);
      append_hydration(span, t12);
      append_hydration(span, t13);
      append_hydration(tr, t14);
    },
    p(new_ctx, dirty) {
      ctx = new_ctx;
      if (dirty & /*$nodes*/
      2 && t0_value !== (t0_value = /*node*/
      (ctx[2].blobServerHTTPAddress || "anonymous") + ""))
        set_data(t0, t0_value);
      if (dirty & /*$nodes*/
      2 && t2_value !== (t2_value = /*node*/
      ctx[2].source + ""))
        set_data(t2, t2_value);
      if (dirty & /*$nodes*/
      2 && t4_value !== (t4_value = /*node*/
      ctx[2].name + ""))
        set_data(t4, t4_value);
      if (dirty & /*$nodes*/
      2 && t6_value !== (t6_value = /*node*/
      ctx[2].ip + ""))
        set_data(t6, t6_value);
      if (dirty & /*$nodes*/
      2 && t9_value !== (t9_value = new Date(
        /*node*/
        ctx[2].connectedAt
      ).toISOString().replace("T", " ") + ""))
        set_data(t9, t9_value);
      if (dirty & /*$nodes*/
      2 && t12_value !== (t12_value = humanTime(
        /*node*/
        ctx[2].connectedAt
      ) + ""))
        set_data(t12, t12_value);
    },
    d(detaching) {
      if (detaching)
        detach(tr);
    }
  };
}
function create_fragment$3(ctx) {
  let div1;
  let header;
  let p;
  let t0;
  let t1;
  let div0;
  function select_block_type(ctx2, dirty) {
    if (
      /*$error*/
      ctx2[0]
    )
      return create_if_block$1;
    if (
      /*$nodes*/
      ctx2[1].length === 0
    )
      return create_if_block_1$1;
    return create_else_block$1;
  }
  let current_block_type = select_block_type(ctx);
  let if_block = current_block_type(ctx);
  return {
    c() {
      div1 = element("div");
      header = element("header");
      p = element("p");
      t0 = text("Connected nodes");
      t1 = space();
      div0 = element("div");
      if_block.c();
      this.h();
    },
    l(nodes2) {
      div1 = claim_element(nodes2, "DIV", { class: true });
      var div1_nodes = children(div1);
      header = claim_element(div1_nodes, "HEADER", { class: true });
      var header_nodes = children(header);
      p = claim_element(header_nodes, "P", { class: true });
      var p_nodes = children(p);
      t0 = claim_text(p_nodes, "Connected nodes");
      p_nodes.forEach(detach);
      header_nodes.forEach(detach);
      t1 = claim_space(div1_nodes);
      div0 = claim_element(div1_nodes, "DIV", { class: true });
      var div0_nodes = children(div0);
      if_block.l(div0_nodes);
      div0_nodes.forEach(detach);
      div1_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(p, "class", "card-header-title");
      attr(header, "class", "card-header");
      attr(div0, "class", "card-content");
      attr(div1, "class", "card panel");
    },
    m(target, anchor) {
      insert_hydration(target, div1, anchor);
      append_hydration(div1, header);
      append_hydration(header, p);
      append_hydration(p, t0);
      append_hydration(div1, t1);
      append_hydration(div1, div0);
      if_block.m(div0, null);
    },
    p(ctx2, [dirty]) {
      if (current_block_type === (current_block_type = select_block_type(ctx2)) && if_block) {
        if_block.p(ctx2, dirty);
      } else {
        if_block.d(1);
        if_block = current_block_type(ctx2);
        if (if_block) {
          if_block.c();
          if_block.m(div0, null);
        }
      }
    },
    i: noop,
    o: noop,
    d(detaching) {
      if (detaching)
        detach(div1);
      if_block.d();
    }
  };
}
function humanTime(time) {
  let diff = (/* @__PURE__ */ new Date()).getTime() - new Date(time).getTime();
  diff = diff / 1e3;
  const days = Math.floor(diff / 86400);
  const hours = Math.floor(diff % 86400 / 3600);
  const minutes = Math.floor(diff % 86400 % 3600 / 60);
  const seconds = Math.floor(diff % 86400 % 3600 % 60);
  let difference = "";
  if (days > 0) {
    difference += days;
    difference += " day" + (days === 1 ? ", " : "s, ");
    difference += hours;
    difference += " hour" + (hours === 1 ? ", " : "s, ");
    difference += minutes;
    difference += " minute" + (minutes === 1 ? " and " : "s and ");
    difference += seconds;
    difference += " second" + (seconds === 1 ? "" : "s");
    return difference;
  }
  if (hours > 0) {
    difference += hours;
    difference += " hour" + (hours === 1 ? ", " : "s, ");
    difference += minutes;
    difference += " minute" + (minutes === 1 ? " and " : "s and ");
    difference += seconds;
    difference += " second" + (seconds === 1 ? "" : "s");
    return difference;
  }
  if (minutes > 0) {
    difference += minutes;
    difference += " minute" + (minutes === 1 ? " and " : "s and ");
    difference += seconds;
    difference += " second" + (seconds === 1 ? "" : "s");
    return difference;
  }
  if (seconds > 0) {
    difference += seconds;
    difference += " second" + (seconds === 1 ? "" : "s");
    return difference;
  }
  return "0 seconds";
}
function instance$2($$self, $$props, $$invalidate) {
  let $error;
  let $nodes;
  component_subscribe($$self, error, ($$value) => $$invalidate(0, $error = $$value));
  component_subscribe($$self, nodes, ($$value) => $$invalidate(1, $nodes = $$value));
  return [$error, $nodes];
}
class ConnectedNodes extends SvelteComponent {
  constructor(options) {
    super();
    init(this, options, instance$2, create_fragment$3, safe_not_equal, {});
  }
}
const ChaintipTrackerNode_svelte_svelte_type_style_lang = "";
function create_fragment$2(ctx) {
  let tr;
  let td0;
  let t0_value = (
    /*node*/
    (ctx[0].source || "anonymous") + ""
  );
  let t0;
  let t1;
  let td1;
  let t2_value = (
    /*node*/
    ctx[0].name + ""
  );
  let t2;
  let t3;
  let td2;
  let t4_value = (
    /*node*/
    ctx[0].blobServerHTTPAddress + ""
  );
  let t4;
  let t5;
  let td3;
  let t6_value = (
    /*node*/
    ctx[0].header.height + ""
  );
  let t6;
  let t7;
  let td4;
  let t8_value = shortHash(
    /*node*/
    ctx[0].header.hash
  ) + "";
  let t8;
  let td4_title_value;
  let t9;
  let td5;
  let t10_value = shortHash(
    /*node*/
    ctx[0].header.previousblockhash
  ) + "";
  let t10;
  let td5_title_value;
  return {
    c() {
      tr = element("tr");
      td0 = element("td");
      t0 = text(t0_value);
      t1 = space();
      td1 = element("td");
      t2 = text(t2_value);
      t3 = space();
      td2 = element("td");
      t4 = text(t4_value);
      t5 = space();
      td3 = element("td");
      t6 = text(t6_value);
      t7 = space();
      td4 = element("td");
      t8 = text(t8_value);
      t9 = space();
      td5 = element("td");
      t10 = text(t10_value);
      this.h();
    },
    l(nodes2) {
      tr = claim_element(nodes2, "TR", {});
      var tr_nodes = children(tr);
      td0 = claim_element(tr_nodes, "TD", { class: true });
      var td0_nodes = children(td0);
      t0 = claim_text(td0_nodes, t0_value);
      td0_nodes.forEach(detach);
      t1 = claim_space(tr_nodes);
      td1 = claim_element(tr_nodes, "TD", { class: true });
      var td1_nodes = children(td1);
      t2 = claim_text(td1_nodes, t2_value);
      td1_nodes.forEach(detach);
      t3 = claim_space(tr_nodes);
      td2 = claim_element(tr_nodes, "TD", { class: true });
      var td2_nodes = children(td2);
      t4 = claim_text(td2_nodes, t4_value);
      td2_nodes.forEach(detach);
      t5 = claim_space(tr_nodes);
      td3 = claim_element(tr_nodes, "TD", { class: true });
      var td3_nodes = children(td3);
      t6 = claim_text(td3_nodes, t6_value);
      td3_nodes.forEach(detach);
      t7 = claim_space(tr_nodes);
      td4 = claim_element(tr_nodes, "TD", { title: true, class: true });
      var td4_nodes = children(td4);
      t8 = claim_text(td4_nodes, t8_value);
      td4_nodes.forEach(detach);
      t9 = claim_space(tr_nodes);
      td5 = claim_element(tr_nodes, "TD", { title: true, class: true });
      var td5_nodes = children(td5);
      t10 = claim_text(td5_nodes, t10_value);
      td5_nodes.forEach(detach);
      tr_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(td0, "class", "svelte-o4rcm");
      attr(td1, "class", "svelte-o4rcm");
      attr(td2, "class", "svelte-o4rcm");
      attr(td3, "class", "right svelte-o4rcm");
      attr(td4, "title", td4_title_value = /*node*/
      ctx[0].header.hash);
      attr(td4, "class", "svelte-o4rcm");
      attr(td5, "title", td5_title_value = /*node*/
      ctx[0].header.previousblockhash);
      attr(td5, "class", "svelte-o4rcm");
    },
    m(target, anchor) {
      insert_hydration(target, tr, anchor);
      append_hydration(tr, td0);
      append_hydration(td0, t0);
      append_hydration(tr, t1);
      append_hydration(tr, td1);
      append_hydration(td1, t2);
      append_hydration(tr, t3);
      append_hydration(tr, td2);
      append_hydration(td2, t4);
      append_hydration(tr, t5);
      append_hydration(tr, td3);
      append_hydration(td3, t6);
      append_hydration(tr, t7);
      append_hydration(tr, td4);
      append_hydration(td4, t8);
      append_hydration(tr, t9);
      append_hydration(tr, td5);
      append_hydration(td5, t10);
    },
    p(ctx2, [dirty]) {
      if (dirty & /*node*/
      1 && t0_value !== (t0_value = /*node*/
      (ctx2[0].source || "anonymous") + ""))
        set_data(t0, t0_value);
      if (dirty & /*node*/
      1 && t2_value !== (t2_value = /*node*/
      ctx2[0].name + ""))
        set_data(t2, t2_value);
      if (dirty & /*node*/
      1 && t4_value !== (t4_value = /*node*/
      ctx2[0].blobServerHTTPAddress + ""))
        set_data(t4, t4_value);
      if (dirty & /*node*/
      1 && t6_value !== (t6_value = /*node*/
      ctx2[0].header.height + ""))
        set_data(t6, t6_value);
      if (dirty & /*node*/
      1 && t8_value !== (t8_value = shortHash(
        /*node*/
        ctx2[0].header.hash
      ) + ""))
        set_data(t8, t8_value);
      if (dirty & /*node*/
      1 && td4_title_value !== (td4_title_value = /*node*/
      ctx2[0].header.hash)) {
        attr(td4, "title", td4_title_value);
      }
      if (dirty & /*node*/
      1 && t10_value !== (t10_value = shortHash(
        /*node*/
        ctx2[0].header.previousblockhash
      ) + ""))
        set_data(t10, t10_value);
      if (dirty & /*node*/
      1 && td5_title_value !== (td5_title_value = /*node*/
      ctx2[0].header.previousblockhash)) {
        attr(td5, "title", td5_title_value);
      }
    },
    i: noop,
    o: noop,
    d(detaching) {
      if (detaching)
        detach(tr);
    }
  };
}
function shortHash(hash) {
  if (!hash)
    return "";
  return hash.substring(0, 8) + "..." + hash.substring(hash.length - 8);
}
function instance$1($$self, $$props, $$invalidate) {
  let { node } = $$props;
  $$self.$$set = ($$props2) => {
    if ("node" in $$props2)
      $$invalidate(0, node = $$props2.node);
  };
  return [node];
}
class ChaintipTrackerNode extends SvelteComponent {
  constructor(options) {
    super();
    init(this, options, instance$1, create_fragment$2, safe_not_equal, { node: 0 });
  }
}
const ChaintipTracker_svelte_svelte_type_style_lang = "";
function get_each_context(ctx, list, i) {
  const child_ctx = ctx.slice();
  child_ctx[2] = list[i];
  return child_ctx;
}
function create_else_block(ctx) {
  let table;
  let thead;
  let tr;
  let th0;
  let t0;
  let t1;
  let th1;
  let t2;
  let t3;
  let th2;
  let t4;
  let t5;
  let th3;
  let t6;
  let t7;
  let th4;
  let t8;
  let t9;
  let th5;
  let t10;
  let t11;
  let tbody;
  let each_blocks = [];
  let each_1_lookup = /* @__PURE__ */ new Map();
  let current;
  let each_value = (
    /*$nodes*/
    ctx[1]
  );
  const get_key = (ctx2) => (
    /*node*/
    ctx2[2].ip
  );
  for (let i = 0; i < each_value.length; i += 1) {
    let child_ctx = get_each_context(ctx, each_value, i);
    let key = get_key(child_ctx);
    each_1_lookup.set(key, each_blocks[i] = create_each_block(key, child_ctx));
  }
  return {
    c() {
      table = element("table");
      thead = element("thead");
      tr = element("tr");
      th0 = element("th");
      t0 = text("Source");
      t1 = space();
      th1 = element("th");
      t2 = text("Name");
      t3 = space();
      th2 = element("th");
      t4 = text("Address");
      t5 = space();
      th3 = element("th");
      t6 = text("Height");
      t7 = space();
      th4 = element("th");
      t8 = text("Latest hash");
      t9 = space();
      th5 = element("th");
      t10 = text("Previous hash");
      t11 = space();
      tbody = element("tbody");
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].c();
      }
      this.h();
    },
    l(nodes2) {
      table = claim_element(nodes2, "TABLE", { class: true });
      var table_nodes = children(table);
      thead = claim_element(table_nodes, "THEAD", {});
      var thead_nodes = children(thead);
      tr = claim_element(thead_nodes, "TR", { class: true });
      var tr_nodes = children(tr);
      th0 = claim_element(tr_nodes, "TH", { class: true });
      var th0_nodes = children(th0);
      t0 = claim_text(th0_nodes, "Source");
      th0_nodes.forEach(detach);
      t1 = claim_space(tr_nodes);
      th1 = claim_element(tr_nodes, "TH", { class: true });
      var th1_nodes = children(th1);
      t2 = claim_text(th1_nodes, "Name");
      th1_nodes.forEach(detach);
      t3 = claim_space(tr_nodes);
      th2 = claim_element(tr_nodes, "TH", { class: true });
      var th2_nodes = children(th2);
      t4 = claim_text(th2_nodes, "Address");
      th2_nodes.forEach(detach);
      t5 = claim_space(tr_nodes);
      th3 = claim_element(tr_nodes, "TH", { class: true });
      var th3_nodes = children(th3);
      t6 = claim_text(th3_nodes, "Height");
      th3_nodes.forEach(detach);
      t7 = claim_space(tr_nodes);
      th4 = claim_element(tr_nodes, "TH", { class: true });
      var th4_nodes = children(th4);
      t8 = claim_text(th4_nodes, "Latest hash");
      th4_nodes.forEach(detach);
      t9 = claim_space(tr_nodes);
      th5 = claim_element(tr_nodes, "TH", { class: true });
      var th5_nodes = children(th5);
      t10 = claim_text(th5_nodes, "Previous hash");
      th5_nodes.forEach(detach);
      tr_nodes.forEach(detach);
      thead_nodes.forEach(detach);
      t11 = claim_space(table_nodes);
      tbody = claim_element(table_nodes, "TBODY", {});
      var tbody_nodes = children(tbody);
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].l(tbody_nodes);
      }
      tbody_nodes.forEach(detach);
      table_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(th0, "class", "svelte-x1qltz");
      attr(th1, "class", "svelte-x1qltz");
      attr(th2, "class", "svelte-x1qltz");
      attr(th3, "class", "right svelte-x1qltz");
      attr(th4, "class", "svelte-x1qltz");
      attr(th5, "class", "svelte-x1qltz");
      attr(tr, "class", "svelte-x1qltz");
      attr(table, "class", "table svelte-x1qltz");
    },
    m(target, anchor) {
      insert_hydration(target, table, anchor);
      append_hydration(table, thead);
      append_hydration(thead, tr);
      append_hydration(tr, th0);
      append_hydration(th0, t0);
      append_hydration(tr, t1);
      append_hydration(tr, th1);
      append_hydration(th1, t2);
      append_hydration(tr, t3);
      append_hydration(tr, th2);
      append_hydration(th2, t4);
      append_hydration(tr, t5);
      append_hydration(tr, th3);
      append_hydration(th3, t6);
      append_hydration(tr, t7);
      append_hydration(tr, th4);
      append_hydration(th4, t8);
      append_hydration(tr, t9);
      append_hydration(tr, th5);
      append_hydration(th5, t10);
      append_hydration(table, t11);
      append_hydration(table, tbody);
      for (let i = 0; i < each_blocks.length; i += 1) {
        if (each_blocks[i]) {
          each_blocks[i].m(tbody, null);
        }
      }
      current = true;
    },
    p(ctx2, dirty) {
      if (dirty & /*$nodes*/
      2) {
        each_value = /*$nodes*/
        ctx2[1];
        group_outros();
        each_blocks = update_keyed_each(each_blocks, dirty, get_key, 1, ctx2, each_value, each_1_lookup, tbody, outro_and_destroy_block, create_each_block, null, get_each_context);
        check_outros();
      }
    },
    i(local) {
      if (current)
        return;
      for (let i = 0; i < each_value.length; i += 1) {
        transition_in(each_blocks[i]);
      }
      current = true;
    },
    o(local) {
      for (let i = 0; i < each_blocks.length; i += 1) {
        transition_out(each_blocks[i]);
      }
      current = false;
    },
    d(detaching) {
      if (detaching)
        detach(table);
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].d();
      }
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
    l(nodes2) {
      p = claim_element(nodes2, "P", {});
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
    i: noop,
    o: noop,
    d(detaching) {
      if (detaching)
        detach(p);
    }
  };
}
function create_if_block_1(ctx) {
  let node;
  let current;
  node = new ChaintipTrackerNode({ props: { node: (
    /*node*/
    ctx[2]
  ) } });
  return {
    c() {
      create_component(node.$$.fragment);
    },
    l(nodes2) {
      claim_component(node.$$.fragment, nodes2);
    },
    m(target, anchor) {
      mount_component(node, target, anchor);
      current = true;
    },
    p(ctx2, dirty) {
      const node_changes = {};
      if (dirty & /*$nodes*/
      2)
        node_changes.node = /*node*/
        ctx2[2];
      node.$set(node_changes);
    },
    i(local) {
      if (current)
        return;
      transition_in(node.$$.fragment, local);
      current = true;
    },
    o(local) {
      transition_out(node.$$.fragment, local);
      current = false;
    },
    d(detaching) {
      destroy_component(node, detaching);
    }
  };
}
function create_each_block(key_1, ctx) {
  let first;
  let if_block_anchor;
  let current;
  let if_block = (
    /*node*/
    ctx[2].blobServerHTTPAddress && create_if_block_1(ctx)
  );
  return {
    key: key_1,
    first: null,
    c() {
      first = empty();
      if (if_block)
        if_block.c();
      if_block_anchor = empty();
      this.h();
    },
    l(nodes2) {
      first = empty();
      if (if_block)
        if_block.l(nodes2);
      if_block_anchor = empty();
      this.h();
    },
    h() {
      this.first = first;
    },
    m(target, anchor) {
      insert_hydration(target, first, anchor);
      if (if_block)
        if_block.m(target, anchor);
      insert_hydration(target, if_block_anchor, anchor);
      current = true;
    },
    p(new_ctx, dirty) {
      ctx = new_ctx;
      if (
        /*node*/
        ctx[2].blobServerHTTPAddress
      ) {
        if (if_block) {
          if_block.p(ctx, dirty);
          if (dirty & /*$nodes*/
          2) {
            transition_in(if_block, 1);
          }
        } else {
          if_block = create_if_block_1(ctx);
          if_block.c();
          transition_in(if_block, 1);
          if_block.m(if_block_anchor.parentNode, if_block_anchor);
        }
      } else if (if_block) {
        group_outros();
        transition_out(if_block, 1, 1, () => {
          if_block = null;
        });
        check_outros();
      }
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
        detach(first);
      if (if_block)
        if_block.d(detaching);
      if (detaching)
        detach(if_block_anchor);
    }
  };
}
function create_fragment$1(ctx) {
  let div1;
  let header;
  let p;
  let t0;
  let t1;
  let div0;
  let current_block_type_index;
  let if_block;
  let current;
  const if_block_creators = [create_if_block, create_else_block];
  const if_blocks = [];
  function select_block_type(ctx2, dirty) {
    if (
      /*$error*/
      ctx2[0]
    )
      return 0;
    return 1;
  }
  current_block_type_index = select_block_type(ctx);
  if_block = if_blocks[current_block_type_index] = if_block_creators[current_block_type_index](ctx);
  return {
    c() {
      div1 = element("div");
      header = element("header");
      p = element("p");
      t0 = text("Chaintip tracker");
      t1 = space();
      div0 = element("div");
      if_block.c();
      this.h();
    },
    l(nodes2) {
      div1 = claim_element(nodes2, "DIV", { class: true });
      var div1_nodes = children(div1);
      header = claim_element(div1_nodes, "HEADER", { class: true });
      var header_nodes = children(header);
      p = claim_element(header_nodes, "P", { class: true });
      var p_nodes = children(p);
      t0 = claim_text(p_nodes, "Chaintip tracker");
      p_nodes.forEach(detach);
      header_nodes.forEach(detach);
      t1 = claim_space(div1_nodes);
      div0 = claim_element(div1_nodes, "DIV", { class: true });
      var div0_nodes = children(div0);
      if_block.l(div0_nodes);
      div0_nodes.forEach(detach);
      div1_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(p, "class", "card-header-title");
      attr(header, "class", "card-header");
      attr(div0, "class", "card-content svelte-x1qltz");
      attr(div1, "class", "card panel svelte-x1qltz");
    },
    m(target, anchor) {
      insert_hydration(target, div1, anchor);
      append_hydration(div1, header);
      append_hydration(header, p);
      append_hydration(p, t0);
      append_hydration(div1, t1);
      append_hydration(div1, div0);
      if_blocks[current_block_type_index].m(div0, null);
      current = true;
    },
    p(ctx2, [dirty]) {
      let previous_block_index = current_block_type_index;
      current_block_type_index = select_block_type(ctx2);
      if (current_block_type_index === previous_block_index) {
        if_blocks[current_block_type_index].p(ctx2, dirty);
      } else {
        group_outros();
        transition_out(if_blocks[previous_block_index], 1, 1, () => {
          if_blocks[previous_block_index] = null;
        });
        check_outros();
        if_block = if_blocks[current_block_type_index];
        if (!if_block) {
          if_block = if_blocks[current_block_type_index] = if_block_creators[current_block_type_index](ctx2);
          if_block.c();
        } else {
          if_block.p(ctx2, dirty);
        }
        transition_in(if_block, 1);
        if_block.m(div0, null);
      }
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
        detach(div1);
      if_blocks[current_block_type_index].d();
    }
  };
}
function instance($$self, $$props, $$invalidate) {
  let $error;
  let $nodes;
  component_subscribe($$self, error, ($$value) => $$invalidate(0, $error = $$value));
  component_subscribe($$self, nodes, ($$value) => $$invalidate(1, $nodes = $$value));
  return [$error, $nodes];
}
class ChaintipTracker extends SvelteComponent {
  constructor(options) {
    super();
    init(this, options, instance, create_fragment$1, safe_not_equal, {});
  }
}
function create_fragment(ctx) {
  let div;
  let connectednodes;
  let t;
  let chaintiptracker;
  let current;
  connectednodes = new ConnectedNodes({});
  chaintiptracker = new ChaintipTracker({});
  return {
    c() {
      div = element("div");
      create_component(connectednodes.$$.fragment);
      t = space();
      create_component(chaintiptracker.$$.fragment);
    },
    l(nodes2) {
      div = claim_element(nodes2, "DIV", {});
      var div_nodes = children(div);
      claim_component(connectednodes.$$.fragment, div_nodes);
      t = claim_space(div_nodes);
      claim_component(chaintiptracker.$$.fragment, div_nodes);
      div_nodes.forEach(detach);
    },
    m(target, anchor) {
      insert_hydration(target, div, anchor);
      mount_component(connectednodes, div, null);
      append_hydration(div, t);
      mount_component(chaintiptracker, div, null);
      current = true;
    },
    p: noop,
    i(local) {
      if (current)
        return;
      transition_in(connectednodes.$$.fragment, local);
      transition_in(chaintiptracker.$$.fragment, local);
      current = true;
    },
    o(local) {
      transition_out(connectednodes.$$.fragment, local);
      transition_out(chaintiptracker.$$.fragment, local);
      current = false;
    },
    d(detaching) {
      if (detaching)
        detach(div);
      destroy_component(connectednodes);
      destroy_component(chaintiptracker);
    }
  };
}
class Page extends SvelteComponent {
  constructor(options) {
    super();
    init(this, options, null, create_fragment, safe_not_equal, {});
  }
}
export {
  Page as component
};
//# sourceMappingURL=2.2b53d9d7.js.map
