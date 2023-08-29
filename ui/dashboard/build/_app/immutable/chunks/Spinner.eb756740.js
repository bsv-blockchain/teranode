import { S as SvelteComponent, i as init, s as safe_not_equal, k as element, l as claim_element, m as children, h as detach, n as attr, b as insert_hydration, F as append_hydration, J as noop } from "./index.56839d90.js";
const Spinner_svelte_svelte_type_style_lang = "";
function create_fragment(ctx) {
  let div1;
  let div0;
  return {
    c() {
      div1 = element("div");
      div0 = element("div");
      this.h();
    },
    l(nodes) {
      div1 = claim_element(nodes, "DIV", { class: true });
      var div1_nodes = children(div1);
      div0 = claim_element(div1_nodes, "DIV", { class: true });
      children(div0).forEach(detach);
      div1_nodes.forEach(detach);
      this.h();
    },
    h() {
      attr(div0, "class", "spinner svelte-1nppfad");
      attr(div1, "class", "overlay svelte-1nppfad");
    },
    m(target, anchor) {
      insert_hydration(target, div1, anchor);
      append_hydration(div1, div0);
    },
    p: noop,
    i: noop,
    o: noop,
    d(detaching) {
      if (detaching)
        detach(div1);
    }
  };
}
class Spinner extends SvelteComponent {
  constructor(options) {
    super();
    init(this, options, null, create_fragment, safe_not_equal, {});
  }
}
export {
  Spinner as S
};
//# sourceMappingURL=Spinner.eb756740.js.map
