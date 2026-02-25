(() => {
  const { createApp } = window.Vue || {};
  if (!createApp) {
    console.error("Vue not found for fuke app");
    return;
  }

  const app = createApp({
    data() {
      return {
        prefectures: [],
        cities: [],
        selectedPrefId: null,
        selectedCityId: null,
        officeName: "",

        items: [],
        page: 1,
        pageSize: 12,
        total: 0,

        loading: {
          prefectures: false,
          cities: false,
          search: false,
        },
        error: "",
      };
    },
    computed: {
      loadingAny() {
        return this.loading.prefectures || this.loading.cities || this.loading.search;
      },
      hasAnyCriteria() {
        return !!(this.selectedPrefId || this.selectedCityId || (this.officeName && this.officeName.trim()));
      },
      totalPages() {
        if (this.pageSize <= 0) return 0;
        return Math.max(1, Math.ceil(this.total / this.pageSize));
      },
      hasNextPage() {
        return this.page < this.totalPages;
      },
      currentFilterLabel() {
        const pref = this.prefectures.find((p) => p.pref_id === this.selectedPrefId);
        const city = this.cities.find((c) => c.id === this.selectedCityId);
        const parts = [];
        if (pref) parts.push(`${pref.full_name} (${pref.en_name})`);
        if (city) parts.push(city.name);
        if (this.officeName) parts.push(`Office: ${this.officeName}`);
        if (!parts.length) return "No prefecture / city selected";
        return parts.join(" · ");
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
          const resp = await fetch("/api/fuke/prefectures");
          if (!resp.ok) throw new Error(`Failed to load prefectures: ${resp.status}`);
          this.prefectures = await resp.json();
        } catch (e) {
          console.error(e);
          this.error = "Failed to load prefectures list, please try again later.";
        } finally {
          this.loading.prefectures = false;
        }
      },
      async fetchCities() {
        if (!this.selectedPrefId) {
          this.cities = [];
          this.selectedCityId = null;
          return;
        }
        this.loading.cities = true;
        this.error = "";
        try {
          const resp = await fetch(`/api/fuke/cities?pref_id=${this.selectedPrefId}`);
          if (!resp.ok) throw new Error(`Failed to load cities: ${resp.status}`);
          this.cities = await resp.json();
          if (!this.cities.some((c) => c.id === this.selectedCityId)) {
            this.selectedCityId = null;
          }
        } catch (e) {
          console.error(e);
          this.error = "Failed to load cities list, please try again later.";
        } finally {
          this.loading.cities = false;
        }
      },
      onPrefectureChange() {
        // v-model on <select> will keep value as string; normalize to number
        this.selectedPrefId = this.selectedPrefId ? Number(this.selectedPrefId) : null;
        this.page = 1;
        this.fetchCities();
      },
      onCityChange() {
        this.selectedCityId = this.selectedCityId ? Number(this.selectedCityId) : null;
        this.page = 1;
      },
      async search() {
        this.loading.search = true;
        this.error = "";
        try {
          const searchParams = new URLSearchParams();
          if (this.selectedPrefId) searchParams.set("pref_id", String(this.selectedPrefId));
          if (this.selectedCityId) searchParams.set("city_id", String(this.selectedCityId));
          if (this.officeName && this.officeName.trim()) {
            searchParams.set("jpost_name", this.officeName.trim());
          }
          searchParams.set("page", String(this.page));
          searchParams.set("page_size", String(this.pageSize));

          const resp = await fetch(`/api/fuke/search?${searchParams.toString()}`);
          if (!resp.ok) throw new Error(`Failed to search: ${resp.status}`);
          const data = await resp.json();

          this.items = data.items || [];
          this.total = data.total || 0;
          this.page = data.page || this.page;
          this.pageSize = data.page_size || this.pageSize;
        } catch (e) {
          console.error(e);
          this.error = "Failed to search scenic stamp data, please try again later.";
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
        this.selectedCityId = null;
        this.officeName = "";
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

  app.mount("#fuke-app");
})();

