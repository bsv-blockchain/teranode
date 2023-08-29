import { S as SvelteComponent, i as init, s as safe_not_equal, k as element, q as text, a as space, l as claim_element, m as children, r as claim_text, h as detach, c as claim_space, n as attr, b as insert_hydration, F as append_hydration, J as noop, v as group_outros, d as transition_out, f as check_outros, g as transition_in, K as component_subscribe, e as empty, O as destroy_each, u as set_data, y as create_component, z as claim_component, A as mount_component, B as destroy_component } from "../chunks/index.56839d90.js";
import { e as error, b as blocks } from "../chunks/nodeStore.03e80c9a.js";
function create_fragment$2(ctx) {
  let section;
  let div17;
  let h1;
  let t0;
  let t1;
  let div16;
  let div1;
  let label0;
  let t2;
  let div0;
  let input0;
  let input0_value_value;
  let t3;
  let div3;
  let label1;
  let t4;
  let div2;
  let input1;
  let input1_value_value;
  let t5;
  let div5;
  let label2;
  let t6;
  let div4;
  let input2;
  let input2_value_value;
  let t7;
  let div7;
  let label3;
  let t8;
  let div6;
  let input3;
  let input3_value_value;
  let t9;
  let div9;
  let label4;
  let t10;
  let div8;
  let input4;
  let input4_value_value;
  let t11;
  let div11;
  let label5;
  let t12;
  let div10;
  let input5;
  let input5_value_value;
  let t13;
  let div13;
  let label6;
  let t14;
  let div12;
  let input6;
  let input6_value_value;
  let t15;
  let div15;
  let label7;
  let t16;
  let div14;
  let input7;
  let input7_value_value;
  return {
    c() {
      section = element("section");
      div17 = element("div");
      h1 = element("h1");
      t0 = text("Block Data");
      t1 = space();
      div16 = element("div");
      div1 = element("div");
      label0 = element("label");
      t2 = text("Hash\n					");
      div0 = element("div");
      input0 = element("input");
      t3 = space();
      div3 = element("div");
      label1 = element("label");
      t4 = text("Version\n					");
      div2 = element("div");
      input1 = element("input");
      t5 = space();
      div5 = element("div");
      label2 = element("label");
      t6 = text("Previous Block Hash\n					");
      div4 = element("div");
      input2 = element("input");
      t7 = space();
      div7 = element("div");
      label3 = element("label");
      t8 = text("Merkle Root\n					");
      div6 = element("div");
      input3 = element("input");
      t9 = space();
      div9 = element("div");
      label4 = element("label");
      t10 = text("Time\n					");
      div8 = element("div");
      input4 = element("input");
      t11 = space();
      div11 = element("div");
      label5 = element("label");
      t12 = text("Bits\n					");
      div10 = element("div");
      input5 = element("input");
      t13 = space();
      div13 = element("div");
      label6 = element("label");
      t14 = text("Nonce\n					");
      div12 = element("div");
      input6 = element("input");
      t15 = space();
      div15 = element("div");
      label7 = element("label");
      t16 = text("Height\n					");
      div14 = element("div");
      input7 = element("input");
      this.h();
    },
    l(nodes) {
      section = claim_element(nodes, "SECTION", { class: true });
      var section_nodes = children(section);
      div17 = claim_element(section_nodes, "DIV", { class: true });
      var div17_nodes = children(div17);
      h1 = claim_element(div17_nodes, "H1", { class: true });
      var h1_nodes = children(h1);
      t0 = claim_text(h1_nodes, "Block Data");
      h1_nodes.forEach(detach);
      t1 = claim_space(div17_nodes);
      div16 = claim_element(div17_nodes, "DIV", { class: true });
      var div16_nodes = children(div16);
      div1 = claim_element(div16_nodes, "DIV", { class: true });
      var div1_nodes = children(div1);
      label0 = claim_element(div1_nodes, "LABEL", { class: true });
      var label0_nodes = children(label0);
      t2 = claim_text(label0_nodes, "Hash\n					");
      div0 = claim_element(label0_nodes, "DIV", { class: true });
      var div0_nodes = children(div0);
      input0 = claim_element(div0_nodes, "INPUT", { class: true, type: true });
      div0_nodes.forEach(detach);
      label0_nodes.forEach(detach);
      div1_nodes.forEach(detach);
      t3 = claim_space(div16_nodes);
      div3 = claim_element(div16_nodes, "DIV", { class: true });
      var div3_nodes = children(div3);
      label1 = claim_element(div3_nodes, "LABEL", { class: true });
      var label1_nodes = children(label1);
      t4 = claim_text(label1_nodes, "Version\n					");
      div2 = claim_element(label1_nodes, "DIV", { class: true });
      var div2_nodes = children(div2);
      input1 = claim_element(div2_nodes, "INPUT", { class: true, type: true });
      div2_nodes.forEach(detach);
      label1_nodes.forEach(detach);
      div3_nodes.forEach(detach);
      t5 = claim_space(div16_nodes);
      div5 = claim_element(div16_nodes, "DIV", { class: true });
      var div5_nodes = children(div5);
      label2 = claim_element(div5_nodes, "LABEL", { class: true });
      var label2_nodes = children(label2);
      t6 = claim_text(label2_nodes, "Previous Block Hash\n					");
      div4 = claim_element(label2_nodes, "DIV", { class: true });
      var div4_nodes = children(div4);
      input2 = claim_element(div4_nodes, "INPUT", { class: true, type: true });
      div4_nodes.forEach(detach);
      label2_nodes.forEach(detach);
      div5_nodes.forEach(detach);
      t7 = claim_space(div16_nodes);
      div7 = claim_element(div16_nodes, "DIV", { class: true });
      var div7_nodes = children(div7);
      label3 = claim_element(div7_nodes, "LABEL", { class: true });
      var label3_nodes = children(label3);
      t8 = claim_text(label3_nodes, "Merkle Root\n					");
      div6 = claim_element(label3_nodes, "DIV", { class: true });
      var div6_nodes = children(div6);
      input3 = claim_element(div6_nodes, "INPUT", { class: true, type: true });
      div6_nodes.forEach(detach);
      label3_nodes.forEach(detach);
      div7_nodes.forEach(detach);
      t9 = claim_space(div16_nodes);
      div9 = claim_element(div16_nodes, "DIV", { class: true });
      var div9_nodes = children(div9);
      label4 = claim_element(div9_nodes, "LABEL", { class: true });
      var label4_nodes = children(label4);
      t10 = claim_text(label4_nodes, "Time\n					");
      div8 = claim_element(label4_nodes, "DIV", { class: true });
      var div8_nodes = children(div8);
      input4 = claim_element(div8_nodes, "INPUT", { class: true, type: true });
      div8_nodes.forEach(detach);
      label4_nodes.forEach(detach);
      div9_nodes.forEach(detach);
      t11 = claim_space(div16_nodes);
      div11 = claim_element(div16_nodes, "DIV", { class: true });
      var div11_nodes = children(div11);
      label5 = claim_element(div11_nodes, "LABEL", { class: true });
      var label5_nodes = children(label5);
      t12 = claim_text(label5_nodes, "Bits\n					");
      div10 = claim_element(label5_nodes, "DIV", { class: true });
      var div10_nodes = children(div10);
      input5 = claim_element(div10_nodes, "INPUT", { class: true, type: true });
      div10_nodes.forEach(detach);
      label5_nodes.forEach(detach);
      div11_nodes.forEach(detach);
      t13 = claim_space(div16_nodes);
      div13 = claim_element(div16_nodes, "DIV", { class: true });
      var div13_nodes = children(div13);
      label6 = claim_element(div13_nodes, "LABEL", { class: true });
      var label6_nodes = children(label6);
      t14 = claim_text(label6_nodes, "Nonce\n					");
      div12 = claim_element(label6_nodes, "DIV", { class: true });
      var div12_nodes = children(div12);
      input6 = claim_element(div12_nodes, "INPUT", { class: true, type: true });
      div12_nodes.forEach(detach);
      label6_nodes.forEach(detach);
      div13_nodes.forEach(detach);
      t15 = claim_space(div16_nodes);
      div15 = claim_element(div16_nodes, "DIV", { class: true });
      var div15_nodes = children(div15);
      label7 = claim_element(div15_nodes, "LABEL", { class: true });
      var label7_nodes = children(label7);
      t16 = claim_text(label7_nodes, "Height\n					");
      div14 = claim_element(label7_nodes, "DIV", { class: true });
      var div14_nodes = children(div14);
      input7 = claim_element(div14_nodes, "INPUT", { class: true, type: true });
      div14_nodes.forEach(detach);
      label7_nodes.forEach(detach);
      div15_nodes.forEach(detach);
      div16_nodes.forEach(detach);
      div17_nodes.forEach(detach);
      section_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(h1, "class", "title");
      attr(input0, "class", "input");
      attr(input0, "type", "text");
      input0.value = input0_value_value = /*data*/
      ctx[0].hash;
      input0.readOnly = true;
      attr(div0, "class", "control");
      attr(label0, "class", "label");
      attr(div1, "class", "field");
      attr(input1, "class", "input");
      attr(input1, "type", "number");
      input1.value = input1_value_value = /*data*/
      ctx[0].version;
      input1.readOnly = true;
      attr(div2, "class", "control");
      attr(label1, "class", "label");
      attr(div3, "class", "field");
      attr(input2, "class", "input");
      attr(input2, "type", "text");
      input2.value = input2_value_value = /*data*/
      ctx[0].previousblockhash;
      input2.readOnly = true;
      attr(div4, "class", "control");
      attr(label2, "class", "label");
      attr(div5, "class", "field");
      attr(input3, "class", "input");
      attr(input3, "type", "text");
      input3.value = input3_value_value = /*data*/
      ctx[0].merkleroot;
      input3.readOnly = true;
      attr(div6, "class", "control");
      attr(label3, "class", "label");
      attr(div7, "class", "field");
      attr(input4, "class", "input");
      attr(input4, "type", "number");
      input4.value = input4_value_value = /*data*/
      ctx[0].time;
      input4.readOnly = true;
      attr(div8, "class", "control");
      attr(label4, "class", "label");
      attr(div9, "class", "field");
      attr(input5, "class", "input");
      attr(input5, "type", "text");
      input5.value = input5_value_value = /*data*/
      ctx[0].bits;
      input5.readOnly = true;
      attr(div10, "class", "control");
      attr(label5, "class", "label");
      attr(div11, "class", "field");
      attr(input6, "class", "input");
      attr(input6, "type", "number");
      input6.value = input6_value_value = /*data*/
      ctx[0].nonce;
      input6.readOnly = true;
      attr(div12, "class", "control");
      attr(label6, "class", "label");
      attr(div13, "class", "field");
      attr(input7, "class", "input");
      attr(input7, "type", "number");
      input7.value = input7_value_value = /*data*/
      ctx[0].height;
      input7.readOnly = true;
      attr(div14, "class", "control");
      attr(label7, "class", "label");
      attr(div15, "class", "field");
      attr(div16, "class", "box");
      attr(div17, "class", "container");
      attr(section, "class", "section");
    },
    m(target, anchor) {
      insert_hydration(target, section, anchor);
      append_hydration(section, div17);
      append_hydration(div17, h1);
      append_hydration(h1, t0);
      append_hydration(div17, t1);
      append_hydration(div17, div16);
      append_hydration(div16, div1);
      append_hydration(div1, label0);
      append_hydration(label0, t2);
      append_hydration(label0, div0);
      append_hydration(div0, input0);
      append_hydration(div16, t3);
      append_hydration(div16, div3);
      append_hydration(div3, label1);
      append_hydration(label1, t4);
      append_hydration(label1, div2);
      append_hydration(div2, input1);
      append_hydration(div16, t5);
      append_hydration(div16, div5);
      append_hydration(div5, label2);
      append_hydration(label2, t6);
      append_hydration(label2, div4);
      append_hydration(div4, input2);
      append_hydration(div16, t7);
      append_hydration(div16, div7);
      append_hydration(div7, label3);
      append_hydration(label3, t8);
      append_hydration(label3, div6);
      append_hydration(div6, input3);
      append_hydration(div16, t9);
      append_hydration(div16, div9);
      append_hydration(div9, label4);
      append_hydration(label4, t10);
      append_hydration(label4, div8);
      append_hydration(div8, input4);
      append_hydration(div16, t11);
      append_hydration(div16, div11);
      append_hydration(div11, label5);
      append_hydration(label5, t12);
      append_hydration(label5, div10);
      append_hydration(div10, input5);
      append_hydration(div16, t13);
      append_hydration(div16, div13);
      append_hydration(div13, label6);
      append_hydration(label6, t14);
      append_hydration(label6, div12);
      append_hydration(div12, input6);
      append_hydration(div16, t15);
      append_hydration(div16, div15);
      append_hydration(div15, label7);
      append_hydration(label7, t16);
      append_hydration(label7, div14);
      append_hydration(div14, input7);
    },
    p(ctx2, [dirty]) {
      if (dirty & /*data*/
      1 && input0_value_value !== (input0_value_value = /*data*/
      ctx2[0].hash) && input0.value !== input0_value_value) {
        input0.value = input0_value_value;
      }
      if (dirty & /*data*/
      1 && input1_value_value !== (input1_value_value = /*data*/
      ctx2[0].version) && input1.value !== input1_value_value) {
        input1.value = input1_value_value;
      }
      if (dirty & /*data*/
      1 && input2_value_value !== (input2_value_value = /*data*/
      ctx2[0].previousblockhash) && input2.value !== input2_value_value) {
        input2.value = input2_value_value;
      }
      if (dirty & /*data*/
      1 && input3_value_value !== (input3_value_value = /*data*/
      ctx2[0].merkleroot) && input3.value !== input3_value_value) {
        input3.value = input3_value_value;
      }
      if (dirty & /*data*/
      1 && input4_value_value !== (input4_value_value = /*data*/
      ctx2[0].time) && input4.value !== input4_value_value) {
        input4.value = input4_value_value;
      }
      if (dirty & /*data*/
      1 && input5_value_value !== (input5_value_value = /*data*/
      ctx2[0].bits) && input5.value !== input5_value_value) {
        input5.value = input5_value_value;
      }
      if (dirty & /*data*/
      1 && input6_value_value !== (input6_value_value = /*data*/
      ctx2[0].nonce) && input6.value !== input6_value_value) {
        input6.value = input6_value_value;
      }
      if (dirty & /*data*/
      1 && input7_value_value !== (input7_value_value = /*data*/
      ctx2[0].height) && input7.value !== input7_value_value) {
        input7.value = input7_value_value;
      }
    },
    i: noop,
    o: noop,
    d(detaching) {
      if (detaching)
        detach(section);
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
  let each_1_anchor;
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
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].c();
      }
      each_1_anchor = empty();
    },
    l(nodes) {
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].l(nodes);
      }
      each_1_anchor = empty();
    },
    m(target, anchor) {
      for (let i = 0; i < each_blocks.length; i += 1) {
        if (each_blocks[i]) {
          each_blocks[i].m(target, anchor);
        }
      }
      insert_hydration(target, each_1_anchor, anchor);
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
            each_blocks[i].m(each_1_anchor.parentNode, each_1_anchor);
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
      destroy_each(each_blocks, detaching);
      if (detaching)
        detach(each_1_anchor);
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
  let p;
  let t0_value = (
    /*key*/
    ctx[2] + ""
  );
  let t0;
  let t1;
  let each_1_anchor;
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
      p = element("p");
      t0 = text(t0_value);
      t1 = space();
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].c();
      }
      each_1_anchor = empty();
    },
    l(nodes) {
      p = claim_element(nodes, "P", {});
      var p_nodes = children(p);
      t0 = claim_text(p_nodes, t0_value);
      p_nodes.forEach(detach);
      t1 = claim_space(nodes);
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].l(nodes);
      }
      each_1_anchor = empty();
    },
    m(target, anchor) {
      insert_hydration(target, p, anchor);
      append_hydration(p, t0);
      insert_hydration(target, t1, anchor);
      for (let i = 0; i < each_blocks.length; i += 1) {
        if (each_blocks[i]) {
          each_blocks[i].m(target, anchor);
        }
      }
      insert_hydration(target, each_1_anchor, anchor);
      current = true;
    },
    p(ctx2, dirty) {
      if ((!current || dirty & /*$blocks*/
      2) && t0_value !== (t0_value = /*key*/
      ctx2[2] + ""))
        set_data(t0, t0_value);
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
            each_blocks[i].m(each_1_anchor.parentNode, each_1_anchor);
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
        detach(p);
      if (detaching)
        detach(t1);
      destroy_each(each_blocks, detaching);
      if (detaching)
        detach(each_1_anchor);
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
//# sourceMappingURL=3.addcd401.js.map
