import { S as SvelteComponent, i as init, s as safe_not_equal, k as element, q as text, a as space, l as claim_element, m as children, r as claim_text, c as claim_space, h as detach, n as attr, b as insert_hydration, F as append_hydration, u as set_data, J as noop, v as group_outros, d as transition_out, f as check_outros, g as transition_in, K as component_subscribe, U as destroy_each, y as create_component, z as claim_component, A as mount_component, B as destroy_component } from "../chunks/index.ca9f8ef3.js";
import { e as error, b as blocks } from "../chunks/nodeStore.2a80a2db.js";
const LatestBlocksNode_svelte_svelte_type_style_lang = "";
function create_fragment$2(ctx) {
  let div22;
  let div21;
  let div1;
  let t0_value = (
    /*data*/
    ctx[0].hash + ""
  );
  let t0;
  let t1;
  let div0;
  let t2_value = (
    /*data*/
    ctx[0].height + ""
  );
  let t2;
  let t3;
  let div20;
  let div4;
  let div2;
  let t4;
  let t5;
  let div3;
  let t6_value = (
    /*data*/
    ctx[0].previousblockhash + ""
  );
  let t6;
  let t7;
  let div7;
  let div5;
  let t8;
  let t9;
  let div6;
  let t10_value = (
    /*data*/
    ctx[0].version + ""
  );
  let t10;
  let t11;
  let div10;
  let div8;
  let t12;
  let t13;
  let div9;
  let t14_value = (
    /*data*/
    ctx[0].merkleroot + ""
  );
  let t14;
  let t15;
  let div13;
  let div11;
  let t16;
  let t17;
  let div12;
  let t18_value = (
    /*data*/
    ctx[0].time + ""
  );
  let t18;
  let t19;
  let div16;
  let div14;
  let t20;
  let t21;
  let div15;
  let t22_value = (
    /*data*/
    ctx[0].bits + ""
  );
  let t22;
  let t23;
  let div19;
  let div17;
  let t24;
  let t25;
  let div18;
  let t26_value = (
    /*data*/
    ctx[0].nonce + ""
  );
  let t26;
  return {
    c() {
      div22 = element("div");
      div21 = element("div");
      div1 = element("div");
      t0 = text(t0_value);
      t1 = space();
      div0 = element("div");
      t2 = text(t2_value);
      t3 = space();
      div20 = element("div");
      div4 = element("div");
      div2 = element("div");
      t4 = text("Previous Block:");
      t5 = space();
      div3 = element("div");
      t6 = text(t6_value);
      t7 = space();
      div7 = element("div");
      div5 = element("div");
      t8 = text("Version:");
      t9 = space();
      div6 = element("div");
      t10 = text(t10_value);
      t11 = space();
      div10 = element("div");
      div8 = element("div");
      t12 = text("Merkle Root:");
      t13 = space();
      div9 = element("div");
      t14 = text(t14_value);
      t15 = space();
      div13 = element("div");
      div11 = element("div");
      t16 = text("Time:");
      t17 = space();
      div12 = element("div");
      t18 = text(t18_value);
      t19 = space();
      div16 = element("div");
      div14 = element("div");
      t20 = text("Bits:");
      t21 = space();
      div15 = element("div");
      t22 = text(t22_value);
      t23 = space();
      div19 = element("div");
      div17 = element("div");
      t24 = text("Nonce:");
      t25 = space();
      div18 = element("div");
      t26 = text(t26_value);
      this.h();
    },
    l(nodes) {
      div22 = claim_element(nodes, "DIV", { class: true });
      var div22_nodes = children(div22);
      div21 = claim_element(div22_nodes, "DIV", { class: true });
      var div21_nodes = children(div21);
      div1 = claim_element(div21_nodes, "DIV", { class: true });
      var div1_nodes = children(div1);
      t0 = claim_text(div1_nodes, t0_value);
      t1 = claim_space(div1_nodes);
      div0 = claim_element(div1_nodes, "DIV", { class: true });
      var div0_nodes = children(div0);
      t2 = claim_text(div0_nodes, t2_value);
      div0_nodes.forEach(detach);
      div1_nodes.forEach(detach);
      t3 = claim_space(div21_nodes);
      div20 = claim_element(div21_nodes, "DIV", { class: true });
      var div20_nodes = children(div20);
      div4 = claim_element(div20_nodes, "DIV", { class: true });
      var div4_nodes = children(div4);
      div2 = claim_element(div4_nodes, "DIV", { class: true });
      var div2_nodes = children(div2);
      t4 = claim_text(div2_nodes, "Previous Block:");
      div2_nodes.forEach(detach);
      t5 = claim_space(div4_nodes);
      div3 = claim_element(div4_nodes, "DIV", { class: true });
      var div3_nodes = children(div3);
      t6 = claim_text(div3_nodes, t6_value);
      div3_nodes.forEach(detach);
      div4_nodes.forEach(detach);
      t7 = claim_space(div20_nodes);
      div7 = claim_element(div20_nodes, "DIV", { class: true });
      var div7_nodes = children(div7);
      div5 = claim_element(div7_nodes, "DIV", { class: true });
      var div5_nodes = children(div5);
      t8 = claim_text(div5_nodes, "Version:");
      div5_nodes.forEach(detach);
      t9 = claim_space(div7_nodes);
      div6 = claim_element(div7_nodes, "DIV", { class: true });
      var div6_nodes = children(div6);
      t10 = claim_text(div6_nodes, t10_value);
      div6_nodes.forEach(detach);
      div7_nodes.forEach(detach);
      t11 = claim_space(div20_nodes);
      div10 = claim_element(div20_nodes, "DIV", { class: true });
      var div10_nodes = children(div10);
      div8 = claim_element(div10_nodes, "DIV", { class: true });
      var div8_nodes = children(div8);
      t12 = claim_text(div8_nodes, "Merkle Root:");
      div8_nodes.forEach(detach);
      t13 = claim_space(div10_nodes);
      div9 = claim_element(div10_nodes, "DIV", { class: true });
      var div9_nodes = children(div9);
      t14 = claim_text(div9_nodes, t14_value);
      div9_nodes.forEach(detach);
      div10_nodes.forEach(detach);
      t15 = claim_space(div20_nodes);
      div13 = claim_element(div20_nodes, "DIV", { class: true });
      var div13_nodes = children(div13);
      div11 = claim_element(div13_nodes, "DIV", { class: true });
      var div11_nodes = children(div11);
      t16 = claim_text(div11_nodes, "Time:");
      div11_nodes.forEach(detach);
      t17 = claim_space(div13_nodes);
      div12 = claim_element(div13_nodes, "DIV", { class: true });
      var div12_nodes = children(div12);
      t18 = claim_text(div12_nodes, t18_value);
      div12_nodes.forEach(detach);
      div13_nodes.forEach(detach);
      t19 = claim_space(div20_nodes);
      div16 = claim_element(div20_nodes, "DIV", { class: true });
      var div16_nodes = children(div16);
      div14 = claim_element(div16_nodes, "DIV", { class: true });
      var div14_nodes = children(div14);
      t20 = claim_text(div14_nodes, "Bits:");
      div14_nodes.forEach(detach);
      t21 = claim_space(div16_nodes);
      div15 = claim_element(div16_nodes, "DIV", { class: true });
      var div15_nodes = children(div15);
      t22 = claim_text(div15_nodes, t22_value);
      div15_nodes.forEach(detach);
      div16_nodes.forEach(detach);
      t23 = claim_space(div20_nodes);
      div19 = claim_element(div20_nodes, "DIV", { class: true });
      var div19_nodes = children(div19);
      div17 = claim_element(div19_nodes, "DIV", { class: true });
      var div17_nodes = children(div17);
      t24 = claim_text(div17_nodes, "Nonce:");
      div17_nodes.forEach(detach);
      t25 = claim_space(div19_nodes);
      div18 = claim_element(div19_nodes, "DIV", { class: true });
      var div18_nodes = children(div18);
      t26 = claim_text(div18_nodes, t26_value);
      div18_nodes.forEach(detach);
      div19_nodes.forEach(detach);
      div20_nodes.forEach(detach);
      div21_nodes.forEach(detach);
      div22_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(div0, "class", "row");
      attr(div1, "class", "column");
      attr(div2, "class", "label svelte-psxq9w");
      attr(div3, "class", "data-content svelte-psxq9w");
      attr(div4, "class", "data-cell svelte-psxq9w");
      attr(div5, "class", "label svelte-psxq9w");
      attr(div6, "class", "data-content svelte-psxq9w");
      attr(div7, "class", "data-cell svelte-psxq9w");
      attr(div8, "class", "label svelte-psxq9w");
      attr(div9, "class", "data-content svelte-psxq9w");
      attr(div10, "class", "data-cell svelte-psxq9w");
      attr(div11, "class", "label svelte-psxq9w");
      attr(div12, "class", "data-content svelte-psxq9w");
      attr(div13, "class", "data-cell svelte-psxq9w");
      attr(div14, "class", "label svelte-psxq9w");
      attr(div15, "class", "data-content svelte-psxq9w");
      attr(div16, "class", "data-cell svelte-psxq9w");
      attr(div17, "class", "label svelte-psxq9w");
      attr(div18, "class", "data-content svelte-psxq9w");
      attr(div19, "class", "data-cell svelte-psxq9w");
      attr(div20, "class", "data-grid svelte-psxq9w");
      attr(div21, "class", "content");
      attr(div22, "class", "card-content");
    },
    m(target, anchor) {
      insert_hydration(target, div22, anchor);
      append_hydration(div22, div21);
      append_hydration(div21, div1);
      append_hydration(div1, t0);
      append_hydration(div1, t1);
      append_hydration(div1, div0);
      append_hydration(div0, t2);
      append_hydration(div21, t3);
      append_hydration(div21, div20);
      append_hydration(div20, div4);
      append_hydration(div4, div2);
      append_hydration(div2, t4);
      append_hydration(div4, t5);
      append_hydration(div4, div3);
      append_hydration(div3, t6);
      append_hydration(div20, t7);
      append_hydration(div20, div7);
      append_hydration(div7, div5);
      append_hydration(div5, t8);
      append_hydration(div7, t9);
      append_hydration(div7, div6);
      append_hydration(div6, t10);
      append_hydration(div20, t11);
      append_hydration(div20, div10);
      append_hydration(div10, div8);
      append_hydration(div8, t12);
      append_hydration(div10, t13);
      append_hydration(div10, div9);
      append_hydration(div9, t14);
      append_hydration(div20, t15);
      append_hydration(div20, div13);
      append_hydration(div13, div11);
      append_hydration(div11, t16);
      append_hydration(div13, t17);
      append_hydration(div13, div12);
      append_hydration(div12, t18);
      append_hydration(div20, t19);
      append_hydration(div20, div16);
      append_hydration(div16, div14);
      append_hydration(div14, t20);
      append_hydration(div16, t21);
      append_hydration(div16, div15);
      append_hydration(div15, t22);
      append_hydration(div20, t23);
      append_hydration(div20, div19);
      append_hydration(div19, div17);
      append_hydration(div17, t24);
      append_hydration(div19, t25);
      append_hydration(div19, div18);
      append_hydration(div18, t26);
    },
    p(ctx2, [dirty]) {
      if (dirty & /*data*/
      1 && t0_value !== (t0_value = /*data*/
      ctx2[0].hash + ""))
        set_data(t0, t0_value);
      if (dirty & /*data*/
      1 && t2_value !== (t2_value = /*data*/
      ctx2[0].height + ""))
        set_data(t2, t2_value);
      if (dirty & /*data*/
      1 && t6_value !== (t6_value = /*data*/
      ctx2[0].previousblockhash + ""))
        set_data(t6, t6_value);
      if (dirty & /*data*/
      1 && t10_value !== (t10_value = /*data*/
      ctx2[0].version + ""))
        set_data(t10, t10_value);
      if (dirty & /*data*/
      1 && t14_value !== (t14_value = /*data*/
      ctx2[0].merkleroot + ""))
        set_data(t14, t14_value);
      if (dirty & /*data*/
      1 && t18_value !== (t18_value = /*data*/
      ctx2[0].time + ""))
        set_data(t18, t18_value);
      if (dirty & /*data*/
      1 && t22_value !== (t22_value = /*data*/
      ctx2[0].bits + ""))
        set_data(t22, t22_value);
      if (dirty & /*data*/
      1 && t26_value !== (t26_value = /*data*/
      ctx2[0].nonce + ""))
        set_data(t26, t26_value);
    },
    i: noop,
    o: noop,
    d(detaching) {
      if (detaching)
        detach(div22);
    }
  };
}
function instance$1($$self, $$props, $$invalidate) {
  let { data = {} } = $$props;
  $$self.$$set = ($$props2) => {
    if ("data" in $$props2)
      $$invalidate(0, data = $$props2.data);
  };
  return [data];
}
class LatestBlocksNode extends SvelteComponent {
  constructor(options) {
    super();
    init(this, options, instance$1, create_fragment$2, safe_not_equal, { data: 0 });
  }
}
function get_each_context(ctx, list, i) {
  const child_ctx = ctx.slice();
  child_ctx[2] = list[i][0];
  child_ctx[3] = list[i][1];
  return child_ctx;
}
function get_each_context_1(ctx, list, i) {
  const child_ctx = ctx.slice();
  child_ctx[6] = list[i];
  return child_ctx;
}
function create_else_block(ctx) {
  let div;
  let current;
  let each_value = Object.entries(
    /*$blocks*/
    ctx[1]
  );
  let each_blocks = [];
  for (let i = 0; i < each_value.length; i += 1) {
    each_blocks[i] = create_each_block(get_each_context(ctx, each_value, i));
  }
  const out = (i) => transition_out(each_blocks[i], 1, 1, () => {
    each_blocks[i] = null;
  });
  return {
    c() {
      div = element("div");
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].c();
      }
      this.h();
    },
    l(nodes) {
      div = claim_element(nodes, "DIV", { class: true });
      var div_nodes = children(div);
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].l(div_nodes);
      }
      div_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(div, "class", "box");
    },
    m(target, anchor) {
      insert_hydration(target, div, anchor);
      for (let i = 0; i < each_blocks.length; i += 1) {
        if (each_blocks[i]) {
          each_blocks[i].m(div, null);
        }
      }
      current = true;
    },
    p(ctx2, dirty) {
      if (dirty & /*Object, $blocks*/
      2) {
        each_value = Object.entries(
          /*$blocks*/
          ctx2[1]
        );
        let i;
        for (i = 0; i < each_value.length; i += 1) {
          const child_ctx = get_each_context(ctx2, each_value, i);
          if (each_blocks[i]) {
            each_blocks[i].p(child_ctx, dirty);
            transition_in(each_blocks[i], 1);
          } else {
            each_blocks[i] = create_each_block(child_ctx);
            each_blocks[i].c();
            transition_in(each_blocks[i], 1);
            each_blocks[i].m(div, null);
          }
        }
        group_outros();
        for (i = each_value.length; i < each_blocks.length; i += 1) {
          out(i);
        }
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
      each_blocks = each_blocks.filter(Boolean);
      for (let i = 0; i < each_blocks.length; i += 1) {
        transition_out(each_blocks[i]);
      }
      current = false;
    },
    d(detaching) {
      if (detaching)
        detach(div);
      destroy_each(each_blocks, detaching);
    }
  };
}
function create_if_block(ctx) {
  let p;
  let t0;
  let t1;
  return {
    c() {
      p = element("p");
      t0 = text("Error fetching hashes: ");
      t1 = text(
        /*$error*/
        ctx[0]
      );
    },
    l(nodes) {
      p = claim_element(nodes, "P", {});
      var p_nodes = children(p);
      t0 = claim_text(p_nodes, "Error fetching hashes: ");
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
    i: noop,
    o: noop,
    d(detaching) {
      if (detaching)
        detach(p);
    }
  };
}
function create_each_block_1(ctx) {
  let latestblocksnode;
  let current;
  latestblocksnode = new LatestBlocksNode({ props: { data: (
    /*block*/
    ctx[6]
  ) } });
  return {
    c() {
      create_component(latestblocksnode.$$.fragment);
    },
    l(nodes) {
      claim_component(latestblocksnode.$$.fragment, nodes);
    },
    m(target, anchor) {
      mount_component(latestblocksnode, target, anchor);
      current = true;
    },
    p(ctx2, dirty) {
      const latestblocksnode_changes = {};
      if (dirty & /*$blocks*/
      2)
        latestblocksnode_changes.data = /*block*/
        ctx2[6];
      latestblocksnode.$set(latestblocksnode_changes);
    },
    i(local) {
      if (current)
        return;
      transition_in(latestblocksnode.$$.fragment, local);
      current = true;
    },
    o(local) {
      transition_out(latestblocksnode.$$.fragment, local);
      current = false;
    },
    d(detaching) {
      destroy_component(latestblocksnode, detaching);
    }
  };
}
function create_each_block(ctx) {
  let div;
  let header;
  let p;
  let t0;
  let t1_value = (
    /*key*/
    ctx[2] + ""
  );
  let t1;
  let t2;
  let t3;
  let current;
  let each_value_1 = (
    /*item*/
    ctx[3]
  );
  let each_blocks = [];
  for (let i = 0; i < each_value_1.length; i += 1) {
    each_blocks[i] = create_each_block_1(get_each_context_1(ctx, each_value_1, i));
  }
  const out = (i) => transition_out(each_blocks[i], 1, 1, () => {
    each_blocks[i] = null;
  });
  return {
    c() {
      div = element("div");
      header = element("header");
      p = element("p");
      t0 = text("Chaintip: ");
      t1 = text(t1_value);
      t2 = space();
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].c();
      }
      t3 = space();
      this.h();
    },
    l(nodes) {
      div = claim_element(nodes, "DIV", { class: true });
      var div_nodes = children(div);
      header = claim_element(div_nodes, "HEADER", { class: true });
      var header_nodes = children(header);
      p = claim_element(header_nodes, "P", {});
      var p_nodes = children(p);
      t0 = claim_text(p_nodes, "Chaintip: ");
      t1 = claim_text(p_nodes, t1_value);
      p_nodes.forEach(detach);
      header_nodes.forEach(detach);
      t2 = claim_space(div_nodes);
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].l(div_nodes);
      }
      t3 = claim_space(div_nodes);
      div_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(header, "class", "card-header");
      attr(div, "class", "card");
    },
    m(target, anchor) {
      insert_hydration(target, div, anchor);
      append_hydration(div, header);
      append_hydration(header, p);
      append_hydration(p, t0);
      append_hydration(p, t1);
      append_hydration(div, t2);
      for (let i = 0; i < each_blocks.length; i += 1) {
        if (each_blocks[i]) {
          each_blocks[i].m(div, null);
        }
      }
      append_hydration(div, t3);
      current = true;
    },
    p(ctx2, dirty) {
      if ((!current || dirty & /*$blocks*/
      2) && t1_value !== (t1_value = /*key*/
      ctx2[2] + ""))
        set_data(t1, t1_value);
      if (dirty & /*Object, $blocks*/
      2) {
        each_value_1 = /*item*/
        ctx2[3];
        let i;
        for (i = 0; i < each_value_1.length; i += 1) {
          const child_ctx = get_each_context_1(ctx2, each_value_1, i);
          if (each_blocks[i]) {
            each_blocks[i].p(child_ctx, dirty);
            transition_in(each_blocks[i], 1);
          } else {
            each_blocks[i] = create_each_block_1(child_ctx);
            each_blocks[i].c();
            transition_in(each_blocks[i], 1);
            each_blocks[i].m(div, t3);
          }
        }
        group_outros();
        for (i = each_value_1.length; i < each_blocks.length; i += 1) {
          out(i);
        }
        check_outros();
      }
    },
    i(local) {
      if (current)
        return;
      for (let i = 0; i < each_value_1.length; i += 1) {
        transition_in(each_blocks[i]);
      }
      current = true;
    },
    o(local) {
      each_blocks = each_blocks.filter(Boolean);
      for (let i = 0; i < each_blocks.length; i += 1) {
        transition_out(each_blocks[i]);
      }
      current = false;
    },
    d(detaching) {
      if (detaching)
        detach(div);
      destroy_each(each_blocks, detaching);
    }
  };
}
function create_fragment$1(ctx) {
  let div;
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
      if_blocks[current_block_type_index].m(div, null);
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
        if_block.m(div, null);
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
        detach(div);
      if_blocks[current_block_type_index].d();
    }
  };
}
function instance($$self, $$props, $$invalidate) {
  let $error;
  let $blocks;
  component_subscribe($$self, error, ($$value) => $$invalidate(0, $error = $$value));
  component_subscribe($$self, blocks, ($$value) => $$invalidate(1, $blocks = $$value));
  return [$error, $blocks];
}
class LatestBlocks extends SvelteComponent {
  constructor(options) {
    super();
    init(this, options, instance, create_fragment$1, safe_not_equal, {});
  }
}
function create_fragment(ctx) {
  let div;
  let latestblocks;
  let current;
  latestblocks = new LatestBlocks({});
  return {
    c() {
      div = element("div");
      create_component(latestblocks.$$.fragment);
    },
    l(nodes) {
      div = claim_element(nodes, "DIV", {});
      var div_nodes = children(div);
      claim_component(latestblocks.$$.fragment, div_nodes);
      div_nodes.forEach(detach);
    },
    m(target, anchor) {
      insert_hydration(target, div, anchor);
      mount_component(latestblocks, div, null);
      current = true;
    },
    p: noop,
    i(local) {
      if (current)
        return;
      transition_in(latestblocks.$$.fragment, local);
      current = true;
    },
    o(local) {
      transition_out(latestblocks.$$.fragment, local);
      current = false;
    },
    d(detaching) {
      if (detaching)
        detach(div);
      destroy_component(latestblocks);
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
//# sourceMappingURL=4.2ee7fac5.js.map
