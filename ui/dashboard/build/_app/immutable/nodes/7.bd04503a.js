import { S as SvelteComponent, i as init, s as safe_not_equal, k as element, q as text, a as space, l as claim_element, m as children, r as claim_text, h as detach, c as claim_space, n as attr, P as add_render_callback, b as insert_hydration, F as append_hydration, Q as set_input_value, R as select_option, T as listen, u as set_data, J as noop, O as destroy_each, U as run_all, V as select_value } from "../chunks/index.56839d90.js";
const _page_svelte_svelte_type_style_lang = "";
function get_each_context(ctx, list, i) {
  const child_ctx = ctx.slice();
  child_ctx[2] = list[i];
  return child_ctx;
}
function create_each_block(ctx) {
  let option;
  let t_value = (
    /*type*/
    ctx[2] + ""
  );
  let t;
  return {
    c() {
      option = element("option");
      t = text(t_value);
      this.h();
    },
    l(nodes) {
      option = claim_element(nodes, "OPTION", {});
      var option_nodes = children(option);
      t = claim_text(option_nodes, t_value);
      option_nodes.forEach(detach);
      this.h();
    },
    h() {
      option.__value = /*type*/
      ctx[2];
      option.value = option.__value;
    },
    m(target, anchor) {
      insert_hydration(target, option, anchor);
      append_hydration(option, t);
    },
    p: noop,
    d(detaching) {
      if (detaching)
        detach(option);
    }
  };
}
function create_if_block_1(ctx) {
  let div;
  let pre;
  let t0;
  let t1_value = JSON.stringify(
    /*data*/
    ctx[3],
    null,
    2
  ) + "";
  let t1;
  let t2;
  return {
    c() {
      div = element("div");
      pre = element("pre");
      t0 = text("        ");
      t1 = text(t1_value);
      t2 = text("\n      ");
      this.h();
    },
    l(nodes) {
      div = claim_element(nodes, "DIV", { class: true });
      var div_nodes = children(div);
      pre = claim_element(div_nodes, "PRE", {});
      var pre_nodes = children(pre);
      t0 = claim_text(pre_nodes, "        ");
      t1 = claim_text(pre_nodes, t1_value);
      t2 = claim_text(pre_nodes, "\n      ");
      pre_nodes.forEach(detach);
      div_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(div, "class", "box");
    },
    m(target, anchor) {
      insert_hydration(target, div, anchor);
      append_hydration(div, pre);
      append_hydration(pre, t0);
      append_hydration(pre, t1);
      append_hydration(pre, t2);
    },
    p(ctx2, dirty) {
      if (dirty & /*data*/
      8 && t1_value !== (t1_value = JSON.stringify(
        /*data*/
        ctx2[3],
        null,
        2
      ) + ""))
        set_data(t1, t1_value);
    },
    d(detaching) {
      if (detaching)
        detach(div);
    }
  };
}
function create_if_block(ctx) {
  let div;
  let t;
  return {
    c() {
      div = element("div");
      t = text(
        /*error*/
        ctx[4]
      );
      this.h();
    },
    l(nodes) {
      div = claim_element(nodes, "DIV", { class: true });
      var div_nodes = children(div);
      t = claim_text(
        div_nodes,
        /*error*/
        ctx[4]
      );
      div_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(div, "class", "notification is-danger");
    },
    m(target, anchor) {
      insert_hydration(target, div, anchor);
      append_hydration(div, t);
    },
    p(ctx2, dirty) {
      if (dirty & /*error*/
      16)
        set_data(
          t,
          /*error*/
          ctx2[4]
        );
    },
    d(detaching) {
      if (detaching)
        detach(div);
    }
  };
}
function create_fragment(ctx) {
  let section;
  let div0;
  let p;
  let t0;
  let t1;
  let div2;
  let label0;
  let t2;
  let div1;
  let input0;
  let t3;
  let div5;
  let label1;
  let t4;
  let div4;
  let div3;
  let select;
  let option;
  let t5;
  let t6;
  let div7;
  let label2;
  let t7;
  let div6;
  let input1;
  let t8;
  let div9;
  let div8;
  let t9;
  let t10;
  let div11;
  let div10;
  let button;
  let t11;
  let t12;
  let mounted;
  let dispose;
  let each_value = (
    /*itemTypes*/
    ctx[6]
  );
  let each_blocks = [];
  for (let i = 0; i < each_value.length; i += 1) {
    each_blocks[i] = create_each_block(get_each_context(ctx, each_value, i));
  }
  function select_block_type(ctx2, dirty) {
    if (
      /*error*/
      ctx2[4]
    )
      return create_if_block;
    if (
      /*data*/
      ctx2[3]
    )
      return create_if_block_1;
  }
  let current_block_type = select_block_type(ctx);
  let if_block = current_block_type && current_block_type(ctx);
  return {
    c() {
      section = element("section");
      div0 = element("div");
      p = element("p");
      t0 = text("Select the type and enter the hash to fetch data.");
      t1 = space();
      div2 = element("div");
      label0 = element("label");
      t2 = text("HTTP Address\n			");
      div1 = element("div");
      input0 = element("input");
      t3 = space();
      div5 = element("div");
      label1 = element("label");
      t4 = text("Type\n			");
      div4 = element("div");
      div3 = element("div");
      select = element("select");
      option = element("option");
      t5 = text("-- Select Type --");
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].c();
      }
      t6 = space();
      div7 = element("div");
      label2 = element("label");
      t7 = text("Hash\n			");
      div6 = element("div");
      input1 = element("input");
      t8 = space();
      div9 = element("div");
      div8 = element("div");
      t9 = text(
        /*url*/
        ctx[5]
      );
      t10 = space();
      div11 = element("div");
      div10 = element("div");
      button = element("button");
      t11 = text("Fetch");
      t12 = space();
      if (if_block)
        if_block.c();
      this.h();
    },
    l(nodes) {
      section = claim_element(nodes, "SECTION", { class: true });
      var section_nodes = children(section);
      div0 = claim_element(section_nodes, "DIV", { class: true });
      var div0_nodes = children(div0);
      p = claim_element(div0_nodes, "P", {});
      var p_nodes = children(p);
      t0 = claim_text(p_nodes, "Select the type and enter the hash to fetch data.");
      p_nodes.forEach(detach);
      div0_nodes.forEach(detach);
      t1 = claim_space(section_nodes);
      div2 = claim_element(section_nodes, "DIV", { class: true });
      var div2_nodes = children(div2);
      label0 = claim_element(div2_nodes, "LABEL", { class: true });
      var label0_nodes = children(label0);
      t2 = claim_text(label0_nodes, "HTTP Address\n			");
      div1 = claim_element(label0_nodes, "DIV", { class: true });
      var div1_nodes = children(div1);
      input0 = claim_element(div1_nodes, "INPUT", {
        class: true,
        type: true,
        placeholder: true
      });
      div1_nodes.forEach(detach);
      label0_nodes.forEach(detach);
      div2_nodes.forEach(detach);
      t3 = claim_space(section_nodes);
      div5 = claim_element(section_nodes, "DIV", { class: true });
      var div5_nodes = children(div5);
      label1 = claim_element(div5_nodes, "LABEL", { class: true });
      var label1_nodes = children(label1);
      t4 = claim_text(label1_nodes, "Type\n			");
      div4 = claim_element(label1_nodes, "DIV", { class: true });
      var div4_nodes = children(div4);
      div3 = claim_element(div4_nodes, "DIV", { class: true });
      var div3_nodes = children(div3);
      select = claim_element(div3_nodes, "SELECT", {});
      var select_nodes = children(select);
      option = claim_element(select_nodes, "OPTION", {});
      var option_nodes = children(option);
      t5 = claim_text(option_nodes, "-- Select Type --");
      option_nodes.forEach(detach);
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].l(select_nodes);
      }
      select_nodes.forEach(detach);
      div3_nodes.forEach(detach);
      div4_nodes.forEach(detach);
      label1_nodes.forEach(detach);
      div5_nodes.forEach(detach);
      t6 = claim_space(section_nodes);
      div7 = claim_element(section_nodes, "DIV", { class: true });
      var div7_nodes = children(div7);
      label2 = claim_element(div7_nodes, "LABEL", { class: true });
      var label2_nodes = children(label2);
      t7 = claim_text(label2_nodes, "Hash\n			");
      div6 = claim_element(label2_nodes, "DIV", { class: true });
      var div6_nodes = children(div6);
      input1 = claim_element(div6_nodes, "INPUT", {
        class: true,
        type: true,
        placeholder: true
      });
      div6_nodes.forEach(detach);
      label2_nodes.forEach(detach);
      div7_nodes.forEach(detach);
      t8 = claim_space(section_nodes);
      div9 = claim_element(section_nodes, "DIV", { class: true });
      var div9_nodes = children(div9);
      div8 = claim_element(div9_nodes, "DIV", { class: true });
      var div8_nodes = children(div8);
      t9 = claim_text(
        div8_nodes,
        /*url*/
        ctx[5]
      );
      div8_nodes.forEach(detach);
      div9_nodes.forEach(detach);
      t10 = claim_space(section_nodes);
      div11 = claim_element(section_nodes, "DIV", { class: true });
      var div11_nodes = children(div11);
      div10 = claim_element(div11_nodes, "DIV", { class: true });
      var div10_nodes = children(div10);
      button = claim_element(div10_nodes, "BUTTON", { class: true });
      var button_nodes = children(button);
      t11 = claim_text(button_nodes, "Fetch");
      button_nodes.forEach(detach);
      div10_nodes.forEach(detach);
      div11_nodes.forEach(detach);
      t12 = claim_space(section_nodes);
      if (if_block)
        if_block.l(section_nodes);
      section_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(div0, "class", "field");
      attr(input0, "class", "input");
      attr(input0, "type", "text");
      attr(input0, "placeholder", "Enter HTTP address");
      attr(div1, "class", "control");
      attr(label0, "class", "label");
      attr(div2, "class", "field");
      option.__value = "";
      option.value = option.__value;
      if (
        /*type*/
        ctx[2] === void 0
      )
        add_render_callback(() => (
          /*select_change_handler*/
          ctx[9].call(select)
        ));
      attr(div3, "class", "select");
      attr(div4, "class", "control");
      attr(label1, "class", "label");
      attr(div5, "class", "field");
      attr(input1, "class", "input hex-input svelte-1kdjojl");
      attr(input1, "type", "text");
      attr(input1, "placeholder", "Enter ID");
      attr(div6, "class", "control");
      attr(label2, "class", "label");
      attr(div7, "class", "field");
      attr(div8, "class", "control");
      attr(div9, "class", "field");
      attr(button, "class", "button is-primary");
      attr(div10, "class", "control");
      attr(div11, "class", "field");
      attr(section, "class", "section");
    },
    m(target, anchor) {
      insert_hydration(target, section, anchor);
      append_hydration(section, div0);
      append_hydration(div0, p);
      append_hydration(p, t0);
      append_hydration(section, t1);
      append_hydration(section, div2);
      append_hydration(div2, label0);
      append_hydration(label0, t2);
      append_hydration(label0, div1);
      append_hydration(div1, input0);
      set_input_value(
        input0,
        /*addr*/
        ctx[1]
      );
      append_hydration(section, t3);
      append_hydration(section, div5);
      append_hydration(div5, label1);
      append_hydration(label1, t4);
      append_hydration(label1, div4);
      append_hydration(div4, div3);
      append_hydration(div3, select);
      append_hydration(select, option);
      append_hydration(option, t5);
      for (let i = 0; i < each_blocks.length; i += 1) {
        if (each_blocks[i]) {
          each_blocks[i].m(select, null);
        }
      }
      select_option(
        select,
        /*type*/
        ctx[2],
        true
      );
      append_hydration(section, t6);
      append_hydration(section, div7);
      append_hydration(div7, label2);
      append_hydration(label2, t7);
      append_hydration(label2, div6);
      append_hydration(div6, input1);
      set_input_value(
        input1,
        /*hash*/
        ctx[0]
      );
      append_hydration(section, t8);
      append_hydration(section, div9);
      append_hydration(div9, div8);
      append_hydration(div8, t9);
      append_hydration(section, t10);
      append_hydration(section, div11);
      append_hydration(div11, div10);
      append_hydration(div10, button);
      append_hydration(button, t11);
      append_hydration(section, t12);
      if (if_block)
        if_block.m(section, null);
      if (!mounted) {
        dispose = [
          listen(
            input0,
            "input",
            /*input0_input_handler*/
            ctx[8]
          ),
          listen(
            select,
            "change",
            /*select_change_handler*/
            ctx[9]
          ),
          listen(
            input1,
            "input",
            /*input1_input_handler*/
            ctx[10]
          ),
          listen(
            button,
            "click",
            /*fetchData*/
            ctx[7]
          )
        ];
        mounted = true;
      }
    },
    p(ctx2, [dirty]) {
      if (dirty & /*addr*/
      2 && input0.value !== /*addr*/
      ctx2[1]) {
        set_input_value(
          input0,
          /*addr*/
          ctx2[1]
        );
      }
      if (dirty & /*itemTypes*/
      64) {
        each_value = /*itemTypes*/
        ctx2[6];
        let i;
        for (i = 0; i < each_value.length; i += 1) {
          const child_ctx = get_each_context(ctx2, each_value, i);
          if (each_blocks[i]) {
            each_blocks[i].p(child_ctx, dirty);
          } else {
            each_blocks[i] = create_each_block(child_ctx);
            each_blocks[i].c();
            each_blocks[i].m(select, null);
          }
        }
        for (; i < each_blocks.length; i += 1) {
          each_blocks[i].d(1);
        }
        each_blocks.length = each_value.length;
      }
      if (dirty & /*type, itemTypes*/
      68) {
        select_option(
          select,
          /*type*/
          ctx2[2]
        );
      }
      if (dirty & /*hash*/
      1 && input1.value !== /*hash*/
      ctx2[0]) {
        set_input_value(
          input1,
          /*hash*/
          ctx2[0]
        );
      }
      if (dirty & /*url*/
      32)
        set_data(
          t9,
          /*url*/
          ctx2[5]
        );
      if (current_block_type === (current_block_type = select_block_type(ctx2)) && if_block) {
        if_block.p(ctx2, dirty);
      } else {
        if (if_block)
          if_block.d(1);
        if_block = current_block_type && current_block_type(ctx2);
        if (if_block) {
          if_block.c();
          if_block.m(section, null);
        }
      }
    },
    i: noop,
    o: noop,
    d(detaching) {
      if (detaching)
        detach(section);
      destroy_each(each_blocks, detaching);
      if (if_block) {
        if_block.d();
      }
      mounted = false;
      run_all(dispose);
    }
  };
}
function instance($$self, $$props, $$invalidate) {
  let url;
  let hash = "";
  let type = "";
  let data = null;
  let addr = "https://miner1.ubsv.dev:8090";
  let error = null;
  const itemTypes = ["tx", "subtree", "header", "block", "utxo"];
  async function fetchData() {
    try {
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`HTTP error! Status: ${response.status}`);
      }
      $$invalidate(3, data = await response.json());
    } catch (err) {
      $$invalidate(4, error = err.message);
    }
  }
  function input0_input_handler() {
    addr = this.value;
    $$invalidate(1, addr);
  }
  function select_change_handler() {
    type = select_value(this);
    $$invalidate(2, type);
    $$invalidate(6, itemTypes);
  }
  function input1_input_handler() {
    hash = this.value;
    $$invalidate(0, hash);
  }
  $$self.$$.update = () => {
    if ($$self.$$.dirty & /*addr, type, hash*/
    7) {
      $$invalidate(5, url = addr + "/" + type + "/" + hash + "/json");
    }
  };
  return [
    hash,
    addr,
    type,
    data,
    error,
    url,
    itemTypes,
    fetchData,
    input0_input_handler,
    select_change_handler,
    input1_input_handler
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
//# sourceMappingURL=7.bd04503a.js.map
