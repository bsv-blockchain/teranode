import { S as SvelteComponent, i as init, s as safe_not_equal, k as element, q as text, a as space, l as claim_element, m as children, r as claim_text, h as detach, c as claim_space, n as attr, b as insert_hydration, F as append_hydration, u as set_data, J as noop, v as group_outros, L as update_keyed_each, N as outro_and_destroy_block, f as check_outros, g as transition_in, d as transition_out, y as create_component, z as claim_component, A as mount_component, B as destroy_component, e as empty, K as component_subscribe, o as onMount, O as select_value, P as add_render_callback, Q as select_option, R as listen, T as run_all, M as destroy_block } from "../chunks/index.ca9f8ef3.js";
import { n as nodes } from "../chunks/nodeStore.2a80a2db.js";
function create_fragment$2(ctx) {
  let td0;
  let t0_value = (
    /*block*/
    ctx[0].height + ""
  );
  let t0;
  let t1;
  let td1;
  let t2_value = new Date(
    /*block*/
    ctx[0].timestamp
  ).toISOString().replace("T", " ") + "";
  let t2;
  let t3;
  let td2;
  let t4_value = (
    /*block*/
    ctx[0].age + ""
  );
  let t4;
  let t5;
  let td3;
  let t6_value = (
    /*block*/
    ctx[0].deltaTime + ""
  );
  let t6;
  let t7;
  let td4;
  let t8_value = "miner";
  let t8;
  let t9;
  let td5;
  let t10_value = formatNumber(
    /*block*/
    ctx[0].coinbaseValue
  ) + "";
  let t10;
  let t11;
  let td6;
  let t12_value = (
    /*block*/
    ctx[0].transactionCount + ""
  );
  let t12;
  let t13;
  let td7;
  let t14_value = (
    /*block*/
    ctx[0].size + ""
  );
  let t14;
  return {
    c() {
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
      t11 = space();
      td6 = element("td");
      t12 = text(t12_value);
      t13 = space();
      td7 = element("td");
      t14 = text(t14_value);
      this.h();
    },
    l(nodes2) {
      td0 = claim_element(nodes2, "TD", { class: true });
      var td0_nodes = children(td0);
      t0 = claim_text(td0_nodes, t0_value);
      td0_nodes.forEach(detach);
      t1 = claim_space(nodes2);
      td1 = claim_element(nodes2, "TD", {});
      var td1_nodes = children(td1);
      t2 = claim_text(td1_nodes, t2_value);
      td1_nodes.forEach(detach);
      t3 = claim_space(nodes2);
      td2 = claim_element(nodes2, "TD", { class: true });
      var td2_nodes = children(td2);
      t4 = claim_text(td2_nodes, t4_value);
      td2_nodes.forEach(detach);
      t5 = claim_space(nodes2);
      td3 = claim_element(nodes2, "TD", { class: true });
      var td3_nodes = children(td3);
      t6 = claim_text(td3_nodes, t6_value);
      td3_nodes.forEach(detach);
      t7 = claim_space(nodes2);
      td4 = claim_element(nodes2, "TD", {});
      var td4_nodes = children(td4);
      t8 = claim_text(td4_nodes, t8_value);
      td4_nodes.forEach(detach);
      t9 = claim_space(nodes2);
      td5 = claim_element(nodes2, "TD", { class: true });
      var td5_nodes = children(td5);
      t10 = claim_text(td5_nodes, t10_value);
      td5_nodes.forEach(detach);
      t11 = claim_space(nodes2);
      td6 = claim_element(nodes2, "TD", { class: true });
      var td6_nodes = children(td6);
      t12 = claim_text(td6_nodes, t12_value);
      td6_nodes.forEach(detach);
      t13 = claim_space(nodes2);
      td7 = claim_element(nodes2, "TD", { class: true });
      var td7_nodes = children(td7);
      t14 = claim_text(td7_nodes, t14_value);
      td7_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(td0, "class", "has-text-right");
      attr(td2, "class", "has-text-right");
      attr(td3, "class", "has-text-right");
      attr(td5, "class", "has-text-right");
      attr(td6, "class", "has-text-right");
      attr(td7, "class", "has-text-right");
    },
    m(target, anchor) {
      insert_hydration(target, td0, anchor);
      append_hydration(td0, t0);
      insert_hydration(target, t1, anchor);
      insert_hydration(target, td1, anchor);
      append_hydration(td1, t2);
      insert_hydration(target, t3, anchor);
      insert_hydration(target, td2, anchor);
      append_hydration(td2, t4);
      insert_hydration(target, t5, anchor);
      insert_hydration(target, td3, anchor);
      append_hydration(td3, t6);
      insert_hydration(target, t7, anchor);
      insert_hydration(target, td4, anchor);
      append_hydration(td4, t8);
      insert_hydration(target, t9, anchor);
      insert_hydration(target, td5, anchor);
      append_hydration(td5, t10);
      insert_hydration(target, t11, anchor);
      insert_hydration(target, td6, anchor);
      append_hydration(td6, t12);
      insert_hydration(target, t13, anchor);
      insert_hydration(target, td7, anchor);
      append_hydration(td7, t14);
    },
    p(ctx2, [dirty]) {
      if (dirty & /*block*/
      1 && t0_value !== (t0_value = /*block*/
      ctx2[0].height + ""))
        set_data(t0, t0_value);
      if (dirty & /*block*/
      1 && t2_value !== (t2_value = new Date(
        /*block*/
        ctx2[0].timestamp
      ).toISOString().replace("T", " ") + ""))
        set_data(t2, t2_value);
      if (dirty & /*block*/
      1 && t4_value !== (t4_value = /*block*/
      ctx2[0].age + ""))
        set_data(t4, t4_value);
      if (dirty & /*block*/
      1 && t6_value !== (t6_value = /*block*/
      ctx2[0].deltaTime + ""))
        set_data(t6, t6_value);
      if (dirty & /*block*/
      1 && t10_value !== (t10_value = formatNumber(
        /*block*/
        ctx2[0].coinbaseValue
      ) + ""))
        set_data(t10, t10_value);
      if (dirty & /*block*/
      1 && t12_value !== (t12_value = /*block*/
      ctx2[0].transactionCount + ""))
        set_data(t12, t12_value);
      if (dirty & /*block*/
      1 && t14_value !== (t14_value = /*block*/
      ctx2[0].size + ""))
        set_data(t14, t14_value);
    },
    i: noop,
    o: noop,
    d(detaching) {
      if (detaching)
        detach(td0);
      if (detaching)
        detach(t1);
      if (detaching)
        detach(td1);
      if (detaching)
        detach(t3);
      if (detaching)
        detach(td2);
      if (detaching)
        detach(t5);
      if (detaching)
        detach(td3);
      if (detaching)
        detach(t7);
      if (detaching)
        detach(td4);
      if (detaching)
        detach(t9);
      if (detaching)
        detach(td5);
      if (detaching)
        detach(t11);
      if (detaching)
        detach(td6);
      if (detaching)
        detach(t13);
      if (detaching)
        detach(td7);
    }
  };
}
function formatNumber(number) {
  return new Intl.NumberFormat().format(number);
}
function instance$2($$self, $$props, $$invalidate) {
  let { block = {} } = $$props;
  $$self.$$set = ($$props2) => {
    if ("block" in $$props2)
      $$invalidate(0, block = $$props2.block);
  };
  return [block];
}
class BlocksTableRow extends SvelteComponent {
  constructor(options) {
    super();
    init(this, options, instance$2, create_fragment$2, safe_not_equal, { block: 0 });
  }
}
const BlocksTable_svelte_svelte_type_style_lang = "";
function get_each_context$1(ctx, list, i) {
  const child_ctx = ctx.slice();
  child_ctx[1] = list[i];
  return child_ctx;
}
function create_each_block$1(key_1, ctx) {
  let tr;
  let blockstablerow;
  let t;
  let current;
  blockstablerow = new BlocksTableRow({ props: { block: (
    /*block*/
    ctx[1]
  ) } });
  return {
    key: key_1,
    first: null,
    c() {
      tr = element("tr");
      create_component(blockstablerow.$$.fragment);
      t = space();
      this.h();
    },
    l(nodes2) {
      tr = claim_element(nodes2, "TR", { class: true });
      var tr_nodes = children(tr);
      claim_component(blockstablerow.$$.fragment, tr_nodes);
      t = claim_space(tr_nodes);
      tr_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(tr, "class", "svelte-1d96r2b");
      this.first = tr;
    },
    m(target, anchor) {
      insert_hydration(target, tr, anchor);
      mount_component(blockstablerow, tr, null);
      append_hydration(tr, t);
      current = true;
    },
    p(new_ctx, dirty) {
      ctx = new_ctx;
      const blockstablerow_changes = {};
      if (dirty & /*blocks*/
      1)
        blockstablerow_changes.block = /*block*/
        ctx[1];
      blockstablerow.$set(blockstablerow_changes);
    },
    i(local) {
      if (current)
        return;
      transition_in(blockstablerow.$$.fragment, local);
      current = true;
    },
    o(local) {
      transition_out(blockstablerow.$$.fragment, local);
      current = false;
    },
    d(detaching) {
      if (detaching)
        detach(tr);
      destroy_component(blockstablerow);
    }
  };
}
function create_fragment$1(ctx) {
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
  let th6;
  let t12;
  let t13;
  let th7;
  let t14;
  let t15;
  let tbody;
  let each_blocks = [];
  let each_1_lookup = /* @__PURE__ */ new Map();
  let current;
  let each_value = (
    /*blocks*/
    ctx[0]
  );
  const get_key = (ctx2) => (
    /*block*/
    ctx2[1].height + /*block*/
    ctx2[1].hash
  );
  for (let i = 0; i < each_value.length; i += 1) {
    let child_ctx = get_each_context$1(ctx, each_value, i);
    let key = get_key(child_ctx);
    each_1_lookup.set(key, each_blocks[i] = create_each_block$1(key, child_ctx));
  }
  return {
    c() {
      table = element("table");
      thead = element("thead");
      tr = element("tr");
      th0 = element("th");
      t0 = text("Height");
      t1 = space();
      th1 = element("th");
      t2 = text("Timestamp (UTC)");
      t3 = space();
      th2 = element("th");
      t4 = text("Age");
      t5 = space();
      th3 = element("th");
      t6 = text("Delta Time");
      t7 = space();
      th4 = element("th");
      t8 = text("Miner");
      t9 = space();
      th5 = element("th");
      t10 = text("Fees");
      t11 = space();
      th6 = element("th");
      t12 = text("Number of Transactions");
      t13 = space();
      th7 = element("th");
      t14 = text("Size");
      t15 = space();
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
      t0 = claim_text(th0_nodes, "Height");
      th0_nodes.forEach(detach);
      t1 = claim_space(tr_nodes);
      th1 = claim_element(tr_nodes, "TH", { class: true });
      var th1_nodes = children(th1);
      t2 = claim_text(th1_nodes, "Timestamp (UTC)");
      th1_nodes.forEach(detach);
      t3 = claim_space(tr_nodes);
      th2 = claim_element(tr_nodes, "TH", { class: true });
      var th2_nodes = children(th2);
      t4 = claim_text(th2_nodes, "Age");
      th2_nodes.forEach(detach);
      t5 = claim_space(tr_nodes);
      th3 = claim_element(tr_nodes, "TH", { class: true });
      var th3_nodes = children(th3);
      t6 = claim_text(th3_nodes, "Delta Time");
      th3_nodes.forEach(detach);
      t7 = claim_space(tr_nodes);
      th4 = claim_element(tr_nodes, "TH", { class: true });
      var th4_nodes = children(th4);
      t8 = claim_text(th4_nodes, "Miner");
      th4_nodes.forEach(detach);
      t9 = claim_space(tr_nodes);
      th5 = claim_element(tr_nodes, "TH", { class: true });
      var th5_nodes = children(th5);
      t10 = claim_text(th5_nodes, "Fees");
      th5_nodes.forEach(detach);
      t11 = claim_space(tr_nodes);
      th6 = claim_element(tr_nodes, "TH", { class: true });
      var th6_nodes = children(th6);
      t12 = claim_text(th6_nodes, "Number of Transactions");
      th6_nodes.forEach(detach);
      t13 = claim_space(tr_nodes);
      th7 = claim_element(tr_nodes, "TH", { class: true });
      var th7_nodes = children(th7);
      t14 = claim_text(th7_nodes, "Size");
      th7_nodes.forEach(detach);
      tr_nodes.forEach(detach);
      thead_nodes.forEach(detach);
      t15 = claim_space(table_nodes);
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
      attr(th0, "class", "svelte-1d96r2b");
      attr(th1, "class", "svelte-1d96r2b");
      attr(th2, "class", "svelte-1d96r2b");
      attr(th3, "class", "svelte-1d96r2b");
      attr(th4, "class", "svelte-1d96r2b");
      attr(th5, "class", "svelte-1d96r2b");
      attr(th6, "class", "svelte-1d96r2b");
      attr(th7, "class", "svelte-1d96r2b");
      attr(tr, "class", "svelte-1d96r2b");
      attr(table, "class", "table svelte-1d96r2b");
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
      append_hydration(tr, t11);
      append_hydration(tr, th6);
      append_hydration(th6, t12);
      append_hydration(tr, t13);
      append_hydration(tr, th7);
      append_hydration(th7, t14);
      append_hydration(table, t15);
      append_hydration(table, tbody);
      for (let i = 0; i < each_blocks.length; i += 1) {
        if (each_blocks[i]) {
          each_blocks[i].m(tbody, null);
        }
      }
      current = true;
    },
    p(ctx2, [dirty]) {
      if (dirty & /*blocks*/
      1) {
        each_value = /*blocks*/
        ctx2[0];
        group_outros();
        each_blocks = update_keyed_each(each_blocks, dirty, get_key, 1, ctx2, each_value, each_1_lookup, tbody, outro_and_destroy_block, create_each_block$1, null, get_each_context$1);
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
function instance$1($$self, $$props, $$invalidate) {
  let { blocks = [] } = $$props;
  $$self.$$set = ($$props2) => {
    if ("blocks" in $$props2)
      $$invalidate(0, blocks = $$props2.blocks);
  };
  return [blocks];
}
class BlocksTable extends SvelteComponent {
  constructor(options) {
    super();
    init(this, options, instance$1, create_fragment$1, safe_not_equal, { blocks: 0 });
  }
}
const _page_svelte_svelte_type_style_lang = "";
function get_each_context(ctx, list, i) {
  const child_ctx = ctx.slice();
  child_ctx[7] = list[i];
  return child_ctx;
}
function create_else_block(ctx) {
  let section;
  let div;
  let select;
  let option;
  let t0;
  let each_blocks = [];
  let each_1_lookup = /* @__PURE__ */ new Map();
  let t1;
  let blockstable;
  let current;
  let mounted;
  let dispose;
  let each_value = (
    /*urls*/
    ctx[1]
  );
  const get_key = (ctx2) => (
    /*url*/
    ctx2[7]
  );
  for (let i = 0; i < each_value.length; i += 1) {
    let child_ctx = get_each_context(ctx, each_value, i);
    let key = get_key(child_ctx);
    each_1_lookup.set(key, each_blocks[i] = create_each_block(key, child_ctx));
  }
  blockstable = new BlocksTable({ props: { blocks: (
    /*blocks*/
    ctx[2]
  ) } });
  return {
    c() {
      section = element("section");
      div = element("div");
      select = element("select");
      option = element("option");
      t0 = text("Select a URL");
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].c();
      }
      t1 = space();
      create_component(blockstable.$$.fragment);
      this.h();
    },
    l(nodes2) {
      section = claim_element(nodes2, "SECTION", { class: true });
      var section_nodes = children(section);
      div = claim_element(section_nodes, "DIV", { class: true });
      var div_nodes = children(div);
      select = claim_element(div_nodes, "SELECT", {});
      var select_nodes = children(select);
      option = claim_element(select_nodes, "OPTION", {});
      var option_nodes = children(option);
      t0 = claim_text(option_nodes, "Select a URL");
      option_nodes.forEach(detach);
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].l(select_nodes);
      }
      select_nodes.forEach(detach);
      div_nodes.forEach(detach);
      t1 = claim_space(section_nodes);
      claim_component(blockstable.$$.fragment, section_nodes);
      section_nodes.forEach(detach);
      this.h();
    },
    h() {
      option.disabled = true;
      option.__value = "Select a URL";
      option.value = option.__value;
      if (
        /*selectedURL*/
        ctx[0] === void 0
      )
        add_render_callback(() => (
          /*select_change_handler*/
          ctx[5].call(select)
        ));
      attr(div, "class", "select svelte-q00a3d");
      attr(section, "class", "section");
    },
    m(target, anchor) {
      insert_hydration(target, section, anchor);
      append_hydration(section, div);
      append_hydration(div, select);
      append_hydration(select, option);
      append_hydration(option, t0);
      for (let i = 0; i < each_blocks.length; i += 1) {
        if (each_blocks[i]) {
          each_blocks[i].m(select, null);
        }
      }
      select_option(
        select,
        /*selectedURL*/
        ctx[0],
        true
      );
      append_hydration(section, t1);
      mount_component(blockstable, section, null);
      current = true;
      if (!mounted) {
        dispose = [
          listen(
            select,
            "change",
            /*select_change_handler*/
            ctx[5]
          ),
          listen(
            select,
            "change",
            /*change_handler*/
            ctx[6]
          )
        ];
        mounted = true;
      }
    },
    p(ctx2, dirty) {
      if (dirty & /*urls*/
      2) {
        each_value = /*urls*/
        ctx2[1];
        each_blocks = update_keyed_each(each_blocks, dirty, get_key, 1, ctx2, each_value, each_1_lookup, select, destroy_block, create_each_block, null, get_each_context);
      }
      if (dirty & /*selectedURL, urls*/
      3) {
        select_option(
          select,
          /*selectedURL*/
          ctx2[0]
        );
      }
      const blockstable_changes = {};
      if (dirty & /*blocks*/
      4)
        blockstable_changes.blocks = /*blocks*/
        ctx2[2];
      blockstable.$set(blockstable_changes);
    },
    i(local) {
      if (current)
        return;
      transition_in(blockstable.$$.fragment, local);
      current = true;
    },
    o(local) {
      transition_out(blockstable.$$.fragment, local);
      current = false;
    },
    d(detaching) {
      if (detaching)
        detach(section);
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].d();
      }
      destroy_component(blockstable);
      mounted = false;
      run_all(dispose);
    }
  };
}
function create_if_block(ctx) {
  let p;
  let t;
  return {
    c() {
      p = element("p");
      t = text(error);
    },
    l(nodes2) {
      p = claim_element(nodes2, "P", {});
      var p_nodes = children(p);
      t = claim_text(p_nodes, error);
      p_nodes.forEach(detach);
    },
    m(target, anchor) {
      insert_hydration(target, p, anchor);
      append_hydration(p, t);
    },
    p: noop,
    i: noop,
    o: noop,
    d(detaching) {
      if (detaching)
        detach(p);
    }
  };
}
function create_each_block(key_1, ctx) {
  let option;
  let t_value = (
    /*url*/
    ctx[7] + ""
  );
  let t;
  let option_value_value;
  return {
    key: key_1,
    first: null,
    c() {
      option = element("option");
      t = text(t_value);
      this.h();
    },
    l(nodes2) {
      option = claim_element(nodes2, "OPTION", {});
      var option_nodes = children(option);
      t = claim_text(option_nodes, t_value);
      option_nodes.forEach(detach);
      this.h();
    },
    h() {
      option.__value = option_value_value = /*url*/
      ctx[7];
      option.value = option.__value;
      this.first = option;
    },
    m(target, anchor) {
      insert_hydration(target, option, anchor);
      append_hydration(option, t);
    },
    p(new_ctx, dirty) {
      ctx = new_ctx;
      if (dirty & /*urls*/
      2 && t_value !== (t_value = /*url*/
      ctx[7] + ""))
        set_data(t, t_value);
      if (dirty & /*urls*/
      2 && option_value_value !== (option_value_value = /*url*/
      ctx[7])) {
        option.__value = option_value_value;
        option.value = option.__value;
      }
    },
    d(detaching) {
      if (detaching)
        detach(option);
    }
  };
}
function create_fragment(ctx) {
  let current_block_type_index;
  let if_block;
  let if_block_anchor;
  let current;
  const if_block_creators = [create_if_block, create_else_block];
  const if_blocks = [];
  function select_block_type(ctx2, dirty) {
    return 1;
  }
  current_block_type_index = select_block_type();
  if_block = if_blocks[current_block_type_index] = if_block_creators[current_block_type_index](ctx);
  return {
    c() {
      if_block.c();
      if_block_anchor = empty();
    },
    l(nodes2) {
      if_block.l(nodes2);
      if_block_anchor = empty();
    },
    m(target, anchor) {
      if_blocks[current_block_type_index].m(target, anchor);
      insert_hydration(target, if_block_anchor, anchor);
      current = true;
    },
    p(ctx2, [dirty]) {
      if_block.p(ctx2, dirty);
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
      if_blocks[current_block_type_index].d(detaching);
      if (detaching)
        detach(if_block_anchor);
    }
  };
}
let error = "";
function instance($$self, $$props, $$invalidate) {
  let $nodes;
  component_subscribe($$self, nodes, ($$value) => $$invalidate(4, $nodes = $$value));
  let blocks = [];
  let selectedURL = "";
  let urls = [];
  onMount(async () => {
    fetchData(selectedURL);
  });
  async function fetchData(url) {
    try {
      if (!url)
        return;
      const res = await fetch(`${url}/lastblocks?n=11`);
      const b = await res.json();
      b.forEach((block, i) => {
        if (i === b.length - 1) {
          return;
        }
        const prevBlock = b[i + 1];
        const prevBlockTime = new Date(prevBlock.timestamp);
        const blockTime = new Date(block.timestamp);
        const diff = blockTime - prevBlockTime;
        const minutes = Math.floor(diff / (1e3 * 60));
        const seconds = Math.floor(diff % (1e3 * 60) / 1e3);
        if (minutes > 0) {
          block.deltaTime = `${minutes}m${seconds}s`;
        } else {
          block.deltaTime = `${seconds}s`;
        }
      });
      b.forEach((block) => {
        const blockTime = new Date(block.timestamp);
        const now = /* @__PURE__ */ new Date();
        const diff = now - blockTime;
        const days = Math.floor(diff / (1e3 * 60 * 60 * 24));
        const hours = Math.floor(diff % (1e3 * 60 * 60 * 24) / (1e3 * 60 * 60));
        const minutes = Math.floor(diff % (1e3 * 60 * 60) / (1e3 * 60));
        const seconds = Math.floor(diff % (1e3 * 60) / 1e3);
        if (days > 0) {
          block.age = `${days}d${hours}h${minutes}m`;
        } else if (hours > 0) {
          block.age = `${hours}h${minutes}m`;
        } else if (minutes > 0) {
          block.age = `${minutes}m${seconds}s`;
        } else {
          block.age = `${seconds}s`;
        }
      });
      $$invalidate(2, blocks = b.slice(0, 10));
    } catch (err) {
      console.error(err);
    }
  }
  function select_change_handler() {
    selectedURL = select_value(this);
    $$invalidate(0, selectedURL), $$invalidate(4, $nodes), $$invalidate(1, urls);
    $$invalidate(1, urls), $$invalidate(4, $nodes), $$invalidate(0, selectedURL);
  }
  const change_handler = () => fetchData(selectedURL);
  $$self.$$.update = () => {
    if ($$self.$$.dirty & /*$nodes, selectedURL, urls*/
    19) {
      if ($nodes) {
        $$invalidate(1, urls = $nodes.map((node) => node.blobServerHTTPAddress));
        if (!selectedURL && urls.length > 0) {
          $$invalidate(0, selectedURL = urls[0]);
        }
      }
    }
  };
  return [
    selectedURL,
    urls,
    blocks,
    fetchData,
    $nodes,
    select_change_handler,
    change_handler
  ];
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
//# sourceMappingURL=3.59c3d0ea.js.map
