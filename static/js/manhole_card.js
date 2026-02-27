(() => {
  const { createApp } = window.Vue || {};
  if (!createApp) {
    console.error("Vue not found for manhole card app");
    return;
  }

  const app = createApp({
    data() {
      return {
        prefectures: [],
        selectedPrefId: null,
        nameKeyword: "",

        items: [],
        page: 1,
        pageSize: 12,
        total: 0,

        loading: {
          prefectures: false,
          search: false,
        },
        error: "",
      };
    },
    computed: {
      loadingAny() {
        return this.loading.prefectures || this.loading.search;
      },
      hasAnyCriteria() {
        return !!(this.selectedPrefId || (this.nameKeyword && this.nameKeyword.trim()));
      },
      totalPages() {
        if (this.pageSize <= 0) return 0;
        return Math.max(1, Math.ceil(this.total / this.pageSize));
      },
      hasNextPage() {
        return this.page < this.totalPages;
      },
    },
    methods: {
      padPref(value) {
        if (value == null) return "";
        const n = Number(value);
        if (Number.isNaN(n)) return String(value);
        return String(n).padStart(2, "0");
      },
      async fetchPrefectures() {
        this.loading.prefectures = true;
        this.error = "";
        try {
          const resp = await fetch("/api/prefectures");
          if (!resp.ok) throw new Error(`Failed to load prefectures: ${resp.status}`);
          this.prefectures = await resp.json();
        } catch (e) {
          console.error(e);
          this.error = "Failed to load prefectures list, please try again later.";
        } finally {
          this.loading.prefectures = false;
        }
      },
      onPrefectureChange() {
        this.selectedPrefId = this.selectedPrefId ? Number(this.selectedPrefId) : null;
        this.page = 1;
      },
      async search() {
        this.loading.search = true;
        this.error = "";
        try {
          const searchParams = new URLSearchParams();
          if (this.selectedPrefId) searchParams.set("pref_id", String(this.selectedPrefId));
          if (this.nameKeyword && this.nameKeyword.trim()) {
            searchParams.set("name", this.nameKeyword.trim());
          }
          searchParams.set("page", String(this.page));
          searchParams.set("page_size", String(this.pageSize));

          const resp = await fetch(`/api/manhole-card/search?${searchParams.toString()}`);
          if (!resp.ok) throw new Error(`Failed to search: ${resp.status}`);
          const data = await resp.json();

          this.items = data.items || [];
          this.total = data.total || 0;
          this.page = data.page || this.page;
          this.pageSize = data.page_size || this.pageSize;
        } catch (e) {
          console.error(e);
          this.error = "Failed to search manhole card data, please try again later.";
        } finally {
          this.loading.search = false;
        }
      },
      applyFilter() {
        this.page = 1;
        this.search();
      },
      resetFilter() {
        this.selectedPrefId = null;
        this.nameKeyword = "";
        this.page = 1;
        this.total = 0;
        this.items = [];
        this.error = "";
      },
      nextPage() {
        if (!this.hasNextPage || this.loading.search) return;
        this.page += 1;
        this.search();
      },
      prevPage() {
        if (this.page <= 1 || this.loading.search) return;
        this.page -= 1;
        this.search();
      },
    },
    async mounted() {
      await this.fetchPrefectures();
    },
  });

  app.mount("#manhole-card-app");
})();

