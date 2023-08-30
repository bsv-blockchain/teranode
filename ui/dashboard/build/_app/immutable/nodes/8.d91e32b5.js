import { S as SvelteComponent, i as init, s as safe_not_equal, e as empty, b as insert_hydration, g as transition_in, v as group_outros, d as transition_out, f as check_outros, h as detach, k as element, q as text, l as claim_element, m as children, r as claim_text, n as attr, V as null_to_empty, F as append_hydration, u as set_data, J as noop, U as destroy_each, a as space, c as claim_space, L as update_keyed_each, N as outro_and_destroy_block, y as create_component, z as claim_component, A as mount_component, B as destroy_component, P as add_render_callback, Q as select_option, W as set_input_value, R as listen, T as run_all, O as select_value } from "../chunks/index.ca9f8ef3.js";
const JSONTree_svelte_svelte_type_style_lang = "";
function get_each_context$1(ctx, list, i) {
  const child_ctx = ctx.slice();
  child_ctx[1] = list[i][0];
  child_ctx[2] = list[i][1];
  return child_ctx;
}
function get_each_context_1(ctx, list, i) {
  const child_ctx = ctx.slice();
  child_ctx[5] = list[i];
  return child_ctx;
}
function create_if_block$1(ctx) {
  let current_block_type_index;
  let if_block;
  let if_block_anchor;
  let current;
  const if_block_creators = [create_if_block_1$1, create_else_block_1];
  const if_blocks = [];
  function select_block_type(ctx2, dirty) {
    if (typeof /*data*/
    ctx2[0] === "object")
      return 0;
    return 1;
  }
  current_block_type_index = select_block_type(ctx);
  if_block = if_blocks[current_block_type_index] = if_block_creators[current_block_type_index](ctx);
  return {
    c() {
      if_block.c();
      if_block_anchor = empty();
    },
    l(nodes) {
      if_block.l(nodes);
      if_block_anchor = empty();
    },
    m(target, anchor) {
      if_blocks[current_block_type_index].m(target, anchor);
      insert_hydration(target, if_block_anchor, anchor);
      current = true;
    },
    p(ctx2, dirty) {
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
        if_block.m(if_block_anchor.parentNode, if_block_anchor);
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
      if_blocks[current_block_type_index].d(detaching);
      if (detaching)
        detach(if_block_anchor);
    }
  };
}
function create_else_block_1(ctx) {
  let span;
  let t;
  let span_class_value;
  return {
    c() {
      span = element("span");
      t = text(
        /*data*/
        ctx[0]
      );
      this.h();
    },
    l(nodes) {
      span = claim_element(nodes, "SPAN", { class: true });
      var span_nodes = children(span);
      t = claim_text(
        span_nodes,
        /*data*/
        ctx[0]
      );
      span_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(span, "class", span_class_value = null_to_empty(getType(
        /*data*/
        ctx[0]
      )) + " svelte-1ph84yk");
    },
    m(target, anchor) {
      insert_hydration(target, span, anchor);
      append_hydration(span, t);
    },
    p(ctx2, dirty) {
      if (dirty & /*data*/
      1)
        set_data(
          t,
          /*data*/
          ctx2[0]
        );
      if (dirty & /*data*/
      1 && span_class_value !== (span_class_value = null_to_empty(getType(
        /*data*/
        ctx2[0]
      )) + " svelte-1ph84yk")) {
        attr(span, "class", span_class_value);
      }
    },
    i: noop,
    o: noop,
    d(detaching) {
      if (detaching)
        detach(span);
    }
  };
}
function create_if_block_1$1(ctx) {
  let t0;
  let ul;
  let t1;
  let current;
  let each_value = Object.entries(
    /*data*/
    ctx[0]
  );
  let each_blocks = [];
  for (let i = 0; i < each_value.length; i += 1) {
    each_blocks[i] = create_each_block$1(get_each_context$1(ctx, each_value, i));
  }
  const out = (i) => transition_out(each_blocks[i], 1, 1, () => {
    each_blocks[i] = null;
  });
  return {
    c() {
      t0 = text("{\n		");
      ul = element("ul");
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].c();
      }
      t1 = text("\n		}");
      this.h();
    },
    l(nodes) {
      t0 = claim_text(nodes, "{\n		");
      ul = claim_element(nodes, "UL", { class: true });
      var ul_nodes = children(ul);
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].l(ul_nodes);
      }
      ul_nodes.forEach(detach);
      t1 = claim_text(nodes, "\n		}");
      this.h();
    },
    h() {
      attr(ul, "class", "svelte-1ph84yk");
    },
    m(target, anchor) {
      insert_hydration(target, t0, anchor);
      insert_hydration(target, ul, anchor);
      for (let i = 0; i < each_blocks.length; i += 1) {
        if (each_blocks[i]) {
          each_blocks[i].m(ul, null);
        }
      }
      insert_hydration(target, t1, anchor);
      current = true;
    },
    p(ctx2, dirty) {
      if (dirty & /*Object, data, getType*/
      1) {
        each_value = Object.entries(
          /*data*/
          ctx2[0]
        );
        let i;
        for (i = 0; i < each_value.length; i += 1) {
          const child_ctx = get_each_context$1(ctx2, each_value, i);
          if (each_blocks[i]) {
            each_blocks[i].p(child_ctx, dirty);
            transition_in(each_blocks[i], 1);
          } else {
            each_blocks[i] = create_each_block$1(child_ctx);
            each_blocks[i].c();
            transition_in(each_blocks[i], 1);
            each_blocks[i].m(ul, null);
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
        detach(t0);
      if (detaching)
        detach(ul);
      destroy_each(each_blocks, detaching);
      if (detaching)
        detach(t1);
    }
  };
}
function create_else_block(ctx) {
  let span;
  let t_value = (
    /*value*/
    ctx[2] + ""
  );
  let t;
  let span_class_value;
  return {
    c() {
      span = element("span");
      t = text(t_value);
      this.h();
    },
    l(nodes) {
      span = claim_element(nodes, "SPAN", { class: true });
      var span_nodes = children(span);
      t = claim_text(span_nodes, t_value);
      span_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(span, "class", span_class_value = null_to_empty(getType(
        /*value*/
        ctx[2]
      )) + " svelte-1ph84yk");
    },
    m(target, anchor) {
      insert_hydration(target, span, anchor);
      append_hydration(span, t);
    },
    p(ctx2, dirty) {
      if (dirty & /*data*/
      1 && t_value !== (t_value = /*value*/
      ctx2[2] + ""))
        set_data(t, t_value);
      if (dirty & /*data*/
      1 && span_class_value !== (span_class_value = null_to_empty(getType(
        /*value*/
        ctx2[2]
      )) + " svelte-1ph84yk")) {
        attr(span, "class", span_class_value);
      }
    },
    i: noop,
    o: noop,
    d(detaching) {
      if (detaching)
        detach(span);
    }
  };
}
function create_if_block_5(ctx) {
  let span;
  let t_value = (
    /*value*/
    ctx[2] + ""
  );
  let t;
  return {
    c() {
      span = element("span");
      t = text(t_value);
      this.h();
    },
    l(nodes) {
      span = claim_element(nodes, "SPAN", { class: true });
      var span_nodes = children(span);
      t = claim_text(span_nodes, t_value);
      span_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(span, "class", "string2 svelte-1ph84yk");
    },
    m(target, anchor) {
      insert_hydration(target, span, anchor);
      append_hydration(span, t);
    },
    p(ctx2, dirty) {
      if (dirty & /*data*/
      1 && t_value !== (t_value = /*value*/
      ctx2[2] + ""))
        set_data(t, t_value);
    },
    i: noop,
    o: noop,
    d(detaching) {
      if (detaching)
        detach(span);
    }
  };
}
function create_if_block_4(ctx) {
  let span;
  let t0;
  let t1_value = (
    /*value*/
    ctx[2] + ""
  );
  let t1;
  let t2;
  return {
    c() {
      span = element("span");
      t0 = text('"');
      t1 = text(t1_value);
      t2 = text('"');
      this.h();
    },
    l(nodes) {
      span = claim_element(nodes, "SPAN", { class: true });
      var span_nodes = children(span);
      t0 = claim_text(span_nodes, '"');
      t1 = claim_text(span_nodes, t1_value);
      t2 = claim_text(span_nodes, '"');
      span_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(span, "class", "string svelte-1ph84yk");
    },
    m(target, anchor) {
      insert_hydration(target, span, anchor);
      append_hydration(span, t0);
      append_hydration(span, t1);
      append_hydration(span, t2);
    },
    p(ctx2, dirty) {
      if (dirty & /*data*/
      1 && t1_value !== (t1_value = /*value*/
      ctx2[2] + ""))
        set_data(t1, t1_value);
    },
    i: noop,
    o: noop,
    d(detaching) {
      if (detaching)
        detach(span);
    }
  };
}
function create_if_block_3(ctx) {
  let ul;
  let each_blocks = [];
  let each_1_lookup = /* @__PURE__ */ new Map();
  let current;
  let each_value_1 = (
    /*value*/
    ctx[2]
  );
  const get_key = (ctx2) => (
    /*item*/
    ctx2[5]
  );
  for (let i = 0; i < each_value_1.length; i += 1) {
    let child_ctx = get_each_context_1(ctx, each_value_1, i);
    let key = get_key(child_ctx);
    each_1_lookup.set(key, each_blocks[i] = create_each_block_1(key, child_ctx));
  }
  return {
    c() {
      ul = element("ul");
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].c();
      }
      this.h();
    },
    l(nodes) {
      ul = claim_element(nodes, "UL", { class: true });
      var ul_nodes = children(ul);
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].l(ul_nodes);
      }
      ul_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(ul, "class", "svelte-1ph84yk");
    },
    m(target, anchor) {
      insert_hydration(target, ul, anchor);
      for (let i = 0; i < each_blocks.length; i += 1) {
        if (each_blocks[i]) {
          each_blocks[i].m(ul, null);
        }
      }
      current = true;
    },
    p(ctx2, dirty) {
      if (dirty & /*Object, data*/
      1) {
        each_value_1 = /*value*/
        ctx2[2];
        group_outros();
        each_blocks = update_keyed_each(each_blocks, dirty, get_key, 1, ctx2, each_value_1, each_1_lookup, ul, outro_and_destroy_block, create_each_block_1, null, get_each_context_1);
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
      for (let i = 0; i < each_blocks.length; i += 1) {
        transition_out(each_blocks[i]);
      }
      current = false;
    },
    d(detaching) {
      if (detaching)
        detach(ul);
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].d();
      }
    }
  };
}
function create_if_block_2(ctx) {
  let jsontree;
  let current;
  jsontree = new JSONTree({ props: { data: (
    /*value*/
    ctx[2]
  ) } });
  return {
    c() {
      create_component(jsontree.$$.fragment);
    },
    l(nodes) {
      claim_component(jsontree.$$.fragment, nodes);
    },
    m(target, anchor) {
      mount_component(jsontree, target, anchor);
      current = true;
    },
    p(ctx2, dirty) {
      const jsontree_changes = {};
      if (dirty & /*data*/
      1)
        jsontree_changes.data = /*value*/
        ctx2[2];
      jsontree.$set(jsontree_changes);
    },
    i(local) {
      if (current)
        return;
      transition_in(jsontree.$$.fragment, local);
      current = true;
    },
    o(local) {
      transition_out(jsontree.$$.fragment, local);
      current = false;
    },
    d(detaching) {
      destroy_component(jsontree, detaching);
    }
  };
}
function create_each_block_1(key_1, ctx) {
  let li;
  let jsontree;
  let current;
  jsontree = new JSONTree({ props: { data: (
    /*item*/
    ctx[5]
  ) } });
  return {
    key: key_1,
    first: null,
    c() {
      li = element("li");
      create_component(jsontree.$$.fragment);
      this.h();
    },
    l(nodes) {
      li = claim_element(nodes, "LI", {});
      var li_nodes = children(li);
      claim_component(jsontree.$$.fragment, li_nodes);
      li_nodes.forEach(detach);
      this.h();
    },
    h() {
      this.first = li;
    },
    m(target, anchor) {
      insert_hydration(target, li, anchor);
      mount_component(jsontree, li, null);
      current = true;
    },
    p(new_ctx, dirty) {
      ctx = new_ctx;
      const jsontree_changes = {};
      if (dirty & /*data*/
      1)
        jsontree_changes.data = /*item*/
        ctx[5];
      jsontree.$set(jsontree_changes);
    },
    i(local) {
      if (current)
        return;
      transition_in(jsontree.$$.fragment, local);
      current = true;
    },
    o(local) {
      transition_out(jsontree.$$.fragment, local);
      current = false;
    },
    d(detaching) {
      if (detaching)
        detach(li);
      destroy_component(jsontree);
    }
  };
}
function create_each_block$1(ctx) {
  let li;
  let span;
  let t0_value = (
    /*key*/
    ctx[1] + ""
  );
  let t0;
  let t1;
  let t2;
  let show_if;
  let show_if_1;
  let show_if_2;
  let show_if_3;
  let current_block_type_index;
  let if_block;
  let t3;
  let current;
  const if_block_creators = [
    create_if_block_2,
    create_if_block_3,
    create_if_block_4,
    create_if_block_5,
    create_else_block
  ];
  const if_blocks = [];
  function select_block_type_1(ctx2, dirty) {
    if (dirty & /*data*/
    1)
      show_if = null;
    if (dirty & /*data*/
    1)
      show_if_1 = null;
    if (dirty & /*data*/
    1)
      show_if_2 = null;
    if (dirty & /*data*/
    1)
      show_if_3 = null;
    if (show_if == null)
      show_if = !!(getType(
        /*value*/
        ctx2[2]
      ) === "object" && /*value*/
      ctx2[2] !== null);
    if (show_if)
      return 0;
    if (show_if_1 == null)
      show_if_1 = !!(getType(
        /*value*/
        ctx2[2]
      ) === "array");
    if (show_if_1)
      return 1;
    if (show_if_2 == null)
      show_if_2 = !!(getType(
        /*value*/
        ctx2[2]
      ) === "string");
    if (show_if_2)
      return 2;
    if (show_if_3 == null)
      show_if_3 = !!(getType(
        /*value*/
        ctx2[2]
      ) === "number");
    if (show_if_3)
      return 3;
    return 4;
  }
  current_block_type_index = select_block_type_1(ctx, -1);
  if_block = if_blocks[current_block_type_index] = if_block_creators[current_block_type_index](ctx);
  return {
    c() {
      li = element("li");
      span = element("span");
      t0 = text(t0_value);
      t1 = text(":");
      t2 = space();
      if_block.c();
      t3 = space();
      this.h();
    },
    l(nodes) {
      li = claim_element(nodes, "LI", {});
      var li_nodes = children(li);
      span = claim_element(li_nodes, "SPAN", { class: true });
      var span_nodes = children(span);
      t0 = claim_text(span_nodes, t0_value);
      t1 = claim_text(span_nodes, ":");
      span_nodes.forEach(detach);
      t2 = claim_space(li_nodes);
      if_block.l(li_nodes);
      t3 = claim_space(li_nodes);
      li_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(span, "class", "key svelte-1ph84yk");
    },
    m(target, anchor) {
      insert_hydration(target, li, anchor);
      append_hydration(li, span);
      append_hydration(span, t0);
      append_hydration(span, t1);
      append_hydration(li, t2);
      if_blocks[current_block_type_index].m(li, null);
      append_hydration(li, t3);
      current = true;
    },
    p(ctx2, dirty) {
      if ((!current || dirty & /*data*/
      1) && t0_value !== (t0_value = /*key*/
      ctx2[1] + ""))
        set_data(t0, t0_value);
      let previous_block_index = current_block_type_index;
      current_block_type_index = select_block_type_1(ctx2, dirty);
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
        if_block.m(li, t3);
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
        detach(li);
      if_blocks[current_block_type_index].d();
    }
  };
}
function create_fragment$1(ctx) {
  let if_block_anchor;
  let current;
  let if_block = (
    /*data*/
    ctx[0] && create_if_block$1(ctx)
  );
  return {
    c() {
      if (if_block)
        if_block.c();
      if_block_anchor = empty();
    },
    l(nodes) {
      if (if_block)
        if_block.l(nodes);
      if_block_anchor = empty();
    },
    m(target, anchor) {
      if (if_block)
        if_block.m(target, anchor);
      insert_hydration(target, if_block_anchor, anchor);
      current = true;
    },
    p(ctx2, [dirty]) {
      if (
        /*data*/
        ctx2[0]
      ) {
        if (if_block) {
          if_block.p(ctx2, dirty);
          if (dirty & /*data*/
          1) {
            transition_in(if_block, 1);
          }
        } else {
          if_block = create_if_block$1(ctx2);
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
      if (if_block)
        if_block.d(detaching);
      if (detaching)
        detach(if_block_anchor);
    }
  };
}
function getType(value) {
  if (Array.isArray(value))
    return "array";
  return typeof value;
}
function instance$1($$self, $$props, $$invalidate) {
  let { data = {} } = $$props;
  $$self.$$set = ($$props2) => {
    if ("data" in $$props2)
      $$invalidate(0, data = $$props2.data);
  };
  return [data];
}
class JSONTree extends SvelteComponent {
  constructor(options) {
    super();
    init(this, options, instance$1, create_fragment$1, safe_not_equal, { data: 0 });
  }
}
const _page_svelte_svelte_type_style_lang = "";
function get_each_context(ctx, list, i) {
  const child_ctx = ctx.slice();
  child_ctx[1] = list[i];
  return child_ctx;
}
function create_each_block(ctx) {
  let option;
  let t_value = (
    /*type*/
    ctx[1] + ""
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
      ctx[1];
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
  let jsontree;
  let current;
  jsontree = new JSONTree({ props: { data: (
    /*data*/
    ctx[2]
  ) } });
  return {
    c() {
      div = element("div");
      create_component(jsontree.$$.fragment);
      this.h();
    },
    l(nodes) {
      div = claim_element(nodes, "DIV", { class: true });
      var div_nodes = children(div);
      claim_component(jsontree.$$.fragment, div_nodes);
      div_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(div, "class", "data-box svelte-1fkgfwo");
    },
    m(target, anchor) {
      insert_hydration(target, div, anchor);
      mount_component(jsontree, div, null);
      current = true;
    },
    p(ctx2, dirty) {
      const jsontree_changes = {};
      if (dirty & /*data*/
      4)
        jsontree_changes.data = /*data*/
        ctx2[2];
      jsontree.$set(jsontree_changes);
    },
    i(local) {
      if (current)
        return;
      transition_in(jsontree.$$.fragment, local);
      current = true;
    },
    o(local) {
      transition_out(jsontree.$$.fragment, local);
      current = false;
    },
    d(detaching) {
      if (detaching)
        detach(div);
      destroy_component(jsontree);
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
        ctx[3]
      );
      this.h();
    },
    l(nodes) {
      div = claim_element(nodes, "DIV", { class: true });
      var div_nodes = children(div);
      t = claim_text(
        div_nodes,
        /*error*/
        ctx[3]
      );
      div_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(div, "class", "error-message svelte-1fkgfwo");
    },
    m(target, anchor) {
      insert_hydration(target, div, anchor);
      append_hydration(div, t);
    },
    p(ctx2, dirty) {
      if (dirty & /*error*/
      8)
        set_data(
          t,
          /*error*/
          ctx2[3]
        );
    },
    i: noop,
    o: noop,
    d(detaching) {
      if (detaching)
        detach(div);
    }
  };
}
function create_fragment(ctx) {
  let section;
  let div2;
  let div1;
  let div0;
  let select;
  let option;
  let t0;
  let t1;
  let div3;
  let input;
  let t2;
  let div4;
  let button;
  let t3;
  let button_disabled_value;
  let t4;
  let div5;
  let t5;
  let t6;
  let div6;
  let current_block_type_index;
  let if_block;
  let current;
  let mounted;
  let dispose;
  let each_value = (
    /*itemTypes*/
    ctx[5]
  );
  let each_blocks = [];
  for (let i = 0; i < each_value.length; i += 1) {
    each_blocks[i] = create_each_block(get_each_context(ctx, each_value, i));
  }
  const if_block_creators = [create_if_block, create_if_block_1];
  const if_blocks = [];
  function select_block_type(ctx2, dirty) {
    if (
      /*error*/
      ctx2[3]
    )
      return 0;
    if (
      /*data*/
      ctx2[2]
    )
      return 1;
    return -1;
  }
  if (~(current_block_type_index = select_block_type(ctx))) {
    if_block = if_blocks[current_block_type_index] = if_block_creators[current_block_type_index](ctx);
  }
  return {
    c() {
      section = element("section");
      div2 = element("div");
      div1 = element("div");
      div0 = element("div");
      select = element("select");
      option = element("option");
      t0 = text("-- Select Type --");
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].c();
      }
      t1 = space();
      div3 = element("div");
      input = element("input");
      t2 = space();
      div4 = element("div");
      button = element("button");
      t3 = text("Search");
      t4 = space();
      div5 = element("div");
      t5 = text(
        /*url*/
        ctx[4]
      );
      t6 = space();
      div6 = element("div");
      if (if_block)
        if_block.c();
      this.h();
    },
    l(nodes) {
      section = claim_element(nodes, "SECTION", { class: true });
      var section_nodes = children(section);
      div2 = claim_element(section_nodes, "DIV", { class: true });
      var div2_nodes = children(div2);
      div1 = claim_element(div2_nodes, "DIV", { class: true });
      var div1_nodes = children(div1);
      div0 = claim_element(div1_nodes, "DIV", { class: true });
      var div0_nodes = children(div0);
      select = claim_element(div0_nodes, "SELECT", {});
      var select_nodes = children(select);
      option = claim_element(select_nodes, "OPTION", {});
      var option_nodes = children(option);
      t0 = claim_text(option_nodes, "-- Select Type --");
      option_nodes.forEach(detach);
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].l(select_nodes);
      }
      select_nodes.forEach(detach);
      div0_nodes.forEach(detach);
      div1_nodes.forEach(detach);
      div2_nodes.forEach(detach);
      t1 = claim_space(section_nodes);
      div3 = claim_element(section_nodes, "DIV", { class: true });
      var div3_nodes = children(div3);
      input = claim_element(div3_nodes, "INPUT", {
        class: true,
        type: true,
        placeholder: true,
        maxlength: true
      });
      div3_nodes.forEach(detach);
      t2 = claim_space(section_nodes);
      div4 = claim_element(section_nodes, "DIV", { class: true });
      var div4_nodes = children(div4);
      button = claim_element(div4_nodes, "BUTTON", { class: true });
      var button_nodes = children(button);
      t3 = claim_text(button_nodes, "Search");
      button_nodes.forEach(detach);
      div4_nodes.forEach(detach);
      section_nodes.forEach(detach);
      t4 = claim_space(nodes);
      div5 = claim_element(nodes, "DIV", { class: true });
      var div5_nodes = children(div5);
      t5 = claim_text(
        div5_nodes,
        /*url*/
        ctx[4]
      );
      div5_nodes.forEach(detach);
      t6 = claim_space(nodes);
      div6 = claim_element(nodes, "DIV", { class: true });
      var div6_nodes = children(div6);
      if (if_block)
        if_block.l(div6_nodes);
      div6_nodes.forEach(detach);
      this.h();
    },
    h() {
      option.__value = "";
      option.value = option.__value;
      if (
        /*type*/
        ctx[1] === void 0
      )
        add_render_callback(() => (
          /*select_change_handler*/
          ctx[7].call(select)
        ));
      attr(div0, "class", "select");
      attr(div1, "class", "control");
      attr(div2, "class", "search-field svelte-1fkgfwo");
      attr(input, "class", "input search-input svelte-1fkgfwo");
      attr(input, "type", "text");
      attr(input, "placeholder", "Enter hash");
      attr(input, "maxlength", "64");
      attr(div3, "class", "search-field svelte-1fkgfwo");
      attr(button, "class", "button is-info");
      button.disabled = button_disabled_value = /*url*/
      ctx[4] === "";
      attr(div4, "class", "search-field svelte-1fkgfwo");
      attr(section, "class", "search-bar svelte-1fkgfwo");
      attr(div5, "class", "url svelte-1fkgfwo");
      attr(div6, "class", "result svelte-1fkgfwo");
    },
    m(target, anchor) {
      insert_hydration(target, section, anchor);
      append_hydration(section, div2);
      append_hydration(div2, div1);
      append_hydration(div1, div0);
      append_hydration(div0, select);
      append_hydration(select, option);
      append_hydration(option, t0);
      for (let i = 0; i < each_blocks.length; i += 1) {
        if (each_blocks[i]) {
          each_blocks[i].m(select, null);
        }
      }
      select_option(
        select,
        /*type*/
        ctx[1],
        true
      );
      append_hydration(section, t1);
      append_hydration(section, div3);
      append_hydration(div3, input);
      set_input_value(
        input,
        /*hash*/
        ctx[0]
      );
      append_hydration(section, t2);
      append_hydration(section, div4);
      append_hydration(div4, button);
      append_hydration(button, t3);
      insert_hydration(target, t4, anchor);
      insert_hydration(target, div5, anchor);
      append_hydration(div5, t5);
      insert_hydration(target, t6, anchor);
      insert_hydration(target, div6, anchor);
      if (~current_block_type_index) {
        if_blocks[current_block_type_index].m(div6, null);
      }
      current = true;
      if (!mounted) {
        dispose = [
          listen(
            select,
            "change",
            /*select_change_handler*/
            ctx[7]
          ),
          listen(
            input,
            "input",
            /*input_input_handler*/
            ctx[8]
          ),
          listen(
            button,
            "click",
            /*fetchData*/
            ctx[6]
          )
        ];
        mounted = true;
      }
    },
    p(ctx2, [dirty]) {
      if (dirty & /*itemTypes*/
      32) {
        each_value = /*itemTypes*/
        ctx2[5];
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
      34) {
        select_option(
          select,
          /*type*/
          ctx2[1]
        );
      }
      if (dirty & /*hash*/
      1 && input.value !== /*hash*/
      ctx2[0]) {
        set_input_value(
          input,
          /*hash*/
          ctx2[0]
        );
      }
      if (!current || dirty & /*url*/
      16 && button_disabled_value !== (button_disabled_value = /*url*/
      ctx2[4] === "")) {
        button.disabled = button_disabled_value;
      }
      if (!current || dirty & /*url*/
      16)
        set_data(
          t5,
          /*url*/
          ctx2[4]
        );
      let previous_block_index = current_block_type_index;
      current_block_type_index = select_block_type(ctx2);
      if (current_block_type_index === previous_block_index) {
        if (~current_block_type_index) {
          if_blocks[current_block_type_index].p(ctx2, dirty);
        }
      } else {
        if (if_block) {
          group_outros();
          transition_out(if_blocks[previous_block_index], 1, 1, () => {
            if_blocks[previous_block_index] = null;
          });
          check_outros();
        }
        if (~current_block_type_index) {
          if_block = if_blocks[current_block_type_index];
          if (!if_block) {
            if_block = if_blocks[current_block_type_index] = if_block_creators[current_block_type_index](ctx2);
            if_block.c();
          } else {
            if_block.p(ctx2, dirty);
          }
          transition_in(if_block, 1);
          if_block.m(div6, null);
        } else {
          if_block = null;
        }
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
        detach(section);
      destroy_each(each_blocks, detaching);
      if (detaching)
        detach(t4);
      if (detaching)
        detach(div5);
      if (detaching)
        detach(t6);
      if (detaching)
        detach(div6);
      if (~current_block_type_index) {
        if_blocks[current_block_type_index].d();
      }
      mounted = false;
      run_all(dispose);
    }
  };
}
let addr = "https://miner1.ubsv.dev:8090";
function instance($$self, $$props, $$invalidate) {
  let hash = "";
  let type = "";
  let data = null;
  let error = null;
  let url = "";
  const itemTypes = ["tx", "subtree", "header", "block", "utxo"];
  async function fetchData() {
    try {
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`HTTP error! Status: ${response.status}`);
      }
      const d = await response.json();
      d.extra = { name: "Simon", age: 44 };
      $$invalidate(2, data = d);
    } catch (err) {
      $$invalidate(3, error = err.message);
    }
  }
  function select_change_handler() {
    type = select_value(this);
    $$invalidate(1, type);
    $$invalidate(5, itemTypes);
  }
  function input_input_handler() {
    hash = this.value;
    $$invalidate(0, hash);
  }
  $$self.$$.update = () => {
    if ($$self.$$.dirty & /*type, hash*/
    3) {
      if (type && hash && hash.length === 64) {
        $$invalidate(4, url = addr + "/" + type + "/" + hash + "/json");
      } else {
        $$invalidate(4, url = "");
      }
    }
  };
  return [
    hash,
    type,
    data,
    error,
    url,
    itemTypes,
    fetchData,
    select_change_handler,
    input_input_handler
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
//# sourceMappingURL=8.d91e32b5.js.map
