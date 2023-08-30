import { S as SvelteComponent, i as init, s as safe_not_equal, C as create_slot, k as element, q as text, a as space, D as head_selector, l as claim_element, m as children, h as detach, r as claim_text, c as claim_space, n as attr, E as src_url_equal, F as append_hydration, b as insert_hydration, G as update_slot_base, H as get_all_dirty_from_scope, I as get_slot_changes, g as transition_in, d as transition_out } from "../chunks/index.ca9f8ef3.js";
const prerender = true;
const trailingSlash = "always";
const _layout = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  prerender,
  trailingSlash
}, Symbol.toStringTag, { value: "Module" }));
function create_fragment(ctx) {
  let link0;
  let link1;
  let link2;
  let link3;
  let link4;
  let script;
  let script_src_value;
  let style;
  let t0;
  let t1;
  let nav;
  let div0;
  let span1;
  let span0;
  let t2;
  let t3;
  let a0;
  let span2;
  let t4;
  let span3;
  let t5;
  let span4;
  let t6;
  let div2;
  let div1;
  let a1;
  let t7;
  let t8;
  let a2;
  let t9;
  let t10;
  let a3;
  let t11;
  let t12;
  let a4;
  let t13;
  let t14;
  let a5;
  let t15;
  let t16;
  let a6;
  let t17;
  let t18;
  let a7;
  let t19;
  let t20;
  let current;
  const default_slot_template = (
    /*#slots*/
    ctx[1].default
  );
  const default_slot = create_slot(
    default_slot_template,
    ctx,
    /*$$scope*/
    ctx[0],
    null
  );
  return {
    c() {
      link0 = element("link");
      link1 = element("link");
      link2 = element("link");
      link3 = element("link");
      link4 = element("link");
      script = element("script");
      style = element("style");
      t0 = text("body {\n			font-family: 'Roboto Mono', monospace;\n			background-color: #f4f4f4;\n			margin: 0;\n			padding: 0;\n			padding-top: 52px; /* Adjust for the height of the fixed navbar */\n		}\n\n		/* Custom darker blue color */\n		.navbar.is-primary {\n			background-color: #004466; /* Change this value to your preferred shade of dark blue */\n		}\n\n		/* Add more global styles as needed */");
      t1 = space();
      nav = element("nav");
      div0 = element("div");
      span1 = element("span");
      span0 = element("span");
      t2 = text("Teranode");
      t3 = space();
      a0 = element("a");
      span2 = element("span");
      t4 = space();
      span3 = element("span");
      t5 = space();
      span4 = element("span");
      t6 = space();
      div2 = element("div");
      div1 = element("div");
      a1 = element("a");
      t7 = text("Dashboard");
      t8 = space();
      a2 = element("a");
      t9 = text("Tree");
      t10 = space();
      a3 = element("a");
      t11 = text("TreeDemo1");
      t12 = space();
      a4 = element("a");
      t13 = text("TreeDemo2");
      t14 = space();
      a5 = element("a");
      t15 = text("Viewer");
      t16 = space();
      a6 = element("a");
      t17 = text("Blocks");
      t18 = space();
      a7 = element("a");
      t19 = text("Blockchain");
      t20 = space();
      if (default_slot)
        default_slot.c();
      this.h();
    },
    l(nodes) {
      const head_nodes = head_selector("svelte-qrbdcu", document.head);
      link0 = claim_element(head_nodes, "LINK", { rel: true, href: true });
      link1 = claim_element(head_nodes, "LINK", { rel: true, href: true });
      link2 = claim_element(head_nodes, "LINK", { rel: true, href: true, crossorigin: true });
      link3 = claim_element(head_nodes, "LINK", { href: true, rel: true });
      link4 = claim_element(head_nodes, "LINK", { rel: true, href: true, as: true });
      script = claim_element(head_nodes, "SCRIPT", { src: true });
      var script_nodes = children(script);
      script_nodes.forEach(detach);
      style = claim_element(head_nodes, "STYLE", {});
      var style_nodes = children(style);
      t0 = claim_text(style_nodes, "body {\n			font-family: 'Roboto Mono', monospace;\n			background-color: #f4f4f4;\n			margin: 0;\n			padding: 0;\n			padding-top: 52px; /* Adjust for the height of the fixed navbar */\n		}\n\n		/* Custom darker blue color */\n		.navbar.is-primary {\n			background-color: #004466; /* Change this value to your preferred shade of dark blue */\n		}\n\n		/* Add more global styles as needed */");
      style_nodes.forEach(detach);
      head_nodes.forEach(detach);
      t1 = claim_space(nodes);
      nav = claim_element(nodes, "NAV", { class: true, "aria-label": true });
      var nav_nodes = children(nav);
      div0 = claim_element(nav_nodes, "DIV", { class: true });
      var div0_nodes = children(div0);
      span1 = claim_element(div0_nodes, "SPAN", { class: true });
      var span1_nodes = children(span1);
      span0 = claim_element(span1_nodes, "SPAN", { class: true });
      var span0_nodes = children(span0);
      t2 = claim_text(span0_nodes, "Teranode");
      span0_nodes.forEach(detach);
      span1_nodes.forEach(detach);
      div0_nodes.forEach(detach);
      t3 = claim_space(nav_nodes);
      a0 = claim_element(nav_nodes, "A", {
        role: true,
        class: true,
        href: true,
        "aria-label": true,
        "aria-expanded": true,
        "data-target": true
      });
      var a0_nodes = children(a0);
      span2 = claim_element(a0_nodes, "SPAN", { "aria-hidden": true });
      children(span2).forEach(detach);
      t4 = claim_space(a0_nodes);
      span3 = claim_element(a0_nodes, "SPAN", { "aria-hidden": true });
      children(span3).forEach(detach);
      t5 = claim_space(a0_nodes);
      span4 = claim_element(a0_nodes, "SPAN", { "aria-hidden": true });
      children(span4).forEach(detach);
      a0_nodes.forEach(detach);
      t6 = claim_space(nav_nodes);
      div2 = claim_element(nav_nodes, "DIV", { id: true, class: true });
      var div2_nodes = children(div2);
      div1 = claim_element(div2_nodes, "DIV", { class: true });
      var div1_nodes = children(div1);
      a1 = claim_element(div1_nodes, "A", { class: true, href: true });
      var a1_nodes = children(a1);
      t7 = claim_text(a1_nodes, "Dashboard");
      a1_nodes.forEach(detach);
      t8 = claim_space(div1_nodes);
      a2 = claim_element(div1_nodes, "A", { class: true, href: true });
      var a2_nodes = children(a2);
      t9 = claim_text(a2_nodes, "Tree");
      a2_nodes.forEach(detach);
      t10 = claim_space(div1_nodes);
      a3 = claim_element(div1_nodes, "A", { class: true, href: true });
      var a3_nodes = children(a3);
      t11 = claim_text(a3_nodes, "TreeDemo1");
      a3_nodes.forEach(detach);
      t12 = claim_space(div1_nodes);
      a4 = claim_element(div1_nodes, "A", { class: true, href: true });
      var a4_nodes = children(a4);
      t13 = claim_text(a4_nodes, "TreeDemo2");
      a4_nodes.forEach(detach);
      t14 = claim_space(div1_nodes);
      a5 = claim_element(div1_nodes, "A", { class: true, href: true });
      var a5_nodes = children(a5);
      t15 = claim_text(a5_nodes, "Viewer");
      a5_nodes.forEach(detach);
      t16 = claim_space(div1_nodes);
      a6 = claim_element(div1_nodes, "A", { class: true, href: true });
      var a6_nodes = children(a6);
      t17 = claim_text(a6_nodes, "Blocks");
      a6_nodes.forEach(detach);
      t18 = claim_space(div1_nodes);
      a7 = claim_element(div1_nodes, "A", { class: true, href: true });
      var a7_nodes = children(a7);
      t19 = claim_text(a7_nodes, "Blockchain");
      a7_nodes.forEach(detach);
      div1_nodes.forEach(detach);
      div2_nodes.forEach(detach);
      nav_nodes.forEach(detach);
      t20 = claim_space(nodes);
      if (default_slot)
        default_slot.l(nodes);
      this.h();
    },
    h() {
      attr(link0, "rel", "stylesheet");
      attr(link0, "href", "https://cdn.jsdelivr.net/npm/bulma@0.9.3/css/bulma.min.css");
      attr(link1, "rel", "preconnect");
      attr(link1, "href", "https://fonts.googleapis.com");
      attr(link2, "rel", "preconnect");
      attr(link2, "href", "https://fonts.gstatic.com");
      attr(link2, "crossorigin", "");
      attr(link3, "href", "https://fonts.googleapis.com/css2?family=Roboto+Mono&display=swap");
      attr(link3, "rel", "stylesheet");
      attr(link4, "rel", "preload");
      attr(link4, "href", "https://d3js.org/d3.v6.min.js");
      attr(link4, "as", "script");
      if (!src_url_equal(script.src, script_src_value = "https://d3js.org/d3.v6.min.js"))
        attr(script, "src", script_src_value);
      script.defer = true;
      attr(span0, "class", "is-size-4 client");
      attr(span1, "class", "navbar-item has-text-light");
      attr(div0, "class", "navbar-brand");
      attr(span2, "aria-hidden", "true");
      attr(span3, "aria-hidden", "true");
      attr(span4, "aria-hidden", "true");
      attr(a0, "role", "button");
      attr(a0, "class", "navbar-burger");
      attr(a0, "href", "#");
      attr(a0, "aria-label", "menu");
      attr(a0, "aria-expanded", "false");
      attr(a0, "data-target", "navbarBasicExample");
      attr(a1, "class", "navbar-item");
      attr(a1, "href", "/");
      attr(a2, "class", "navbar-item");
      attr(a2, "href", "/tree");
      attr(a3, "class", "navbar-item");
      attr(a3, "href", "/treedemo1");
      attr(a4, "class", "navbar-item");
      attr(a4, "href", "/treedemo2");
      attr(a5, "class", "navbar-item");
      attr(a5, "href", "/viewer");
      attr(a6, "class", "navbar-item");
      attr(a6, "href", "/blocks");
      attr(a7, "class", "navbar-item");
      attr(a7, "href", "/blockchain");
      attr(div1, "class", "navbar-start");
      attr(div2, "id", "navbarBasicExample");
      attr(div2, "class", "navbar-menu");
      attr(nav, "class", "navbar is-fixed-top is-dark");
      attr(nav, "aria-label", "main navigation");
    },
    m(target, anchor) {
      append_hydration(document.head, link0);
      append_hydration(document.head, link1);
      append_hydration(document.head, link2);
      append_hydration(document.head, link3);
      append_hydration(document.head, link4);
      append_hydration(document.head, script);
      append_hydration(document.head, style);
      append_hydration(style, t0);
      insert_hydration(target, t1, anchor);
      insert_hydration(target, nav, anchor);
      append_hydration(nav, div0);
      append_hydration(div0, span1);
      append_hydration(span1, span0);
      append_hydration(span0, t2);
      append_hydration(nav, t3);
      append_hydration(nav, a0);
      append_hydration(a0, span2);
      append_hydration(a0, t4);
      append_hydration(a0, span3);
      append_hydration(a0, t5);
      append_hydration(a0, span4);
      append_hydration(nav, t6);
      append_hydration(nav, div2);
      append_hydration(div2, div1);
      append_hydration(div1, a1);
      append_hydration(a1, t7);
      append_hydration(div1, t8);
      append_hydration(div1, a2);
      append_hydration(a2, t9);
      append_hydration(div1, t10);
      append_hydration(div1, a3);
      append_hydration(a3, t11);
      append_hydration(div1, t12);
      append_hydration(div1, a4);
      append_hydration(a4, t13);
      append_hydration(div1, t14);
      append_hydration(div1, a5);
      append_hydration(a5, t15);
      append_hydration(div1, t16);
      append_hydration(div1, a6);
      append_hydration(a6, t17);
      append_hydration(div1, t18);
      append_hydration(div1, a7);
      append_hydration(a7, t19);
      insert_hydration(target, t20, anchor);
      if (default_slot) {
        default_slot.m(target, anchor);
      }
      current = true;
    },
    p(ctx2, [dirty]) {
      if (default_slot) {
        if (default_slot.p && (!current || dirty & /*$$scope*/
        1)) {
          update_slot_base(
            default_slot,
            default_slot_template,
            ctx2,
            /*$$scope*/
            ctx2[0],
            !current ? get_all_dirty_from_scope(
              /*$$scope*/
              ctx2[0]
            ) : get_slot_changes(
              default_slot_template,
              /*$$scope*/
              ctx2[0],
              dirty,
              null
            ),
            null
          );
        }
      }
    },
    i(local) {
      if (current)
        return;
      transition_in(default_slot, local);
      current = true;
    },
    o(local) {
      transition_out(default_slot, local);
      current = false;
    },
    d(detaching) {
      detach(link0);
      detach(link1);
      detach(link2);
      detach(link3);
      detach(link4);
      detach(script);
      detach(style);
      if (detaching)
        detach(t1);
      if (detaching)
        detach(nav);
      if (detaching)
        detach(t20);
      if (default_slot)
        default_slot.d(detaching);
    }
  };
}
function instance($$self, $$props, $$invalidate) {
  let { $$slots: slots = {}, $$scope } = $$props;
  $$self.$$set = ($$props2) => {
    if ("$$scope" in $$props2)
      $$invalidate(0, $$scope = $$props2.$$scope);
  };
  return [$$scope, slots];
}
class Layout extends SvelteComponent {
  constructor(options) {
    super();
    init(this, options, instance, create_fragment, safe_not_equal, {});
  }
}
export {
  Layout as component,
  _layout as universal
};
//# sourceMappingURL=0.97fc3141.js.map
