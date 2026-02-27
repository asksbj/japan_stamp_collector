import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from core.settings import TMP_ROOT
from etl.runner import TaskRunner
from models.administration import Prefecture, City, Facility
from manhole_card.model import ManholeCard, ManholeCardFacility


logging.basicConfig(level=logging.INFO)


class ManholeCardMigrator(TaskRunner):
    INTERVAL_DAYS = 7

    @classmethod
    def _load_prefectures(cls) -> Dict[str, Prefecture]:
        prefectures = Prefecture.get_all()
        return {p.en_name: p for p in prefectures}

    @classmethod
    def _load_cities_by_pref(cls) -> Dict[int, List[Tuple[str, int]]]:
        cities = City.get_all()
        cities_by_pref: Dict[int, List[Tuple[str, int]]] = {}
        for c in cities:
            if not c.pref_id or not c.name:
                continue
            cities_by_pref.setdefault(c.pref_id, []).append((c.name, c.id))

        for _pref_id, city_list in cities_by_pref.items():
            city_list.sort(key=lambda item: len(item[0]), reverse=True)

        return cities_by_pref

    @staticmethod
    def _looks_like_address(line: str) -> bool:
        """
        Heuristic: whether a line looks like a Japanese address.
        This is intentionally permissive because some address lines don't include the prefecture name.
        """
        if not line:
            return False

        import re

        # contains municipality marker + some digits/address markers
        if re.search(r"[市郡区町村]", line) and re.search(r"[0-9０-９\-－ー丁目番地号字大字]", line):
            return True
        # sometimes address line contains digits but missing 丁目/番地
        if re.search(r"[0-9０-９]", line) and re.search(r"[市郡区町村]", line):
            return True
        return False

    @staticmethod
    def _parse_location(location: str, prefecture_name: str) -> Optional[Tuple[str, str]]:
        if not location:
            return None

        import re

        # Normalize lines and drop obvious noise
        raw_lines = [line.strip() for line in location.split("\n") if line.strip()]
        lines = [l for l in raw_lines if l.lower() != "none"]
        if not lines:
            return None

        # If distribution is suspended, treat as no data to migrate.
        whole_text = "\n".join(lines)
        if "配布を一時中止" in whole_text or "配布を中止" in whole_text:
            return None

        def _clean_common(s: str) -> str:
            # remove leading schedule markers like 【平日】 or 【5～10月】
            s = re.sub(r"^【[^】]*】\s*", "", s)
            # remove inline markers like 【...】
            s = re.sub(r"【[^】]*】", "", s)
            # remove full-width parentheses content
            s = re.sub(r"（[^）]*）", "", s)
            # remove half-width parentheses content
            s = re.sub(r"\([^)]*\)", "", s)
            return s.strip()

        # 1) Determine address line index.
        addr_idx = -1
        if prefecture_name:
            for idx, line in enumerate(lines):
                if prefecture_name in line and ManholeCardMigrator._looks_like_address(line):
                    addr_idx = idx
                    break
            if addr_idx == -1:
                for idx, line in enumerate(lines):
                    if prefecture_name in line:
                        addr_idx = idx
                        break

        if addr_idx == -1:
            # fallback: use last line if it looks like an address
            if len(lines) >= 2 and ManholeCardMigrator._looks_like_address(lines[-1]):
                addr_idx = len(lines) - 1
            else:
                # facility only, no address
                addr_idx = -1

        raw_facility_lines = lines[:addr_idx] if addr_idx >= 0 else lines

        # 2) Build facility candidates by cleaning and skipping noise.
        facility_candidates: List[str] = []
        for l in raw_facility_lines:
            # skip pure markers / notes / phone lines
            if l.startswith("※"):
                continue
            if l.startswith("（問") or l.startswith("(問") or l.startswith("（問い合わせ") or l.startswith("(問い合わせ"):
                continue
            if re.match(r"^[0-9０-９\-\s－ー]+$", l):
                continue

            cleaned = _clean_common(l)
            if cleaned:
                facility_candidates.append(cleaned)

        if not facility_candidates:
            return None

        org_keywords = [
            "役場",
            "市役所",
            "県庁",
            "上下水道局",
            "下水処理センター",
            "浄化センター",
            "水再生センター",
        ]

        def _org_base(s: str) -> str:
            for kw in org_keywords:
                idx_kw = s.find(kw)
                if idx_kw != -1:
                    return s[: idx_kw + len(kw)]
            return ""

        # 3) Decide facility name.
        facility_name: str
        if len(facility_candidates) == 1:
            facility_name = facility_candidates[0]
        else:
            # Prefer common organization base (役場/市役所/上下水道局/...)
            bases = [_org_base(c) for c in facility_candidates]
            bases = [b for b in bases if b]
            if bases and len(set(bases)) == 1:
                facility_name = bases[0]
            else:
                # If first token is common, use it (e.g. "陸別町役場 建設課" + "陸別町役場 警備室")
                first_tokens = set()
                for cand in facility_candidates:
                    parts = re.split(r"[\s　]+", cand)
                    token = parts[0] if parts and parts[0] else ""
                    if token:
                        first_tokens.add(token)
                if len(first_tokens) == 1:
                    facility_name = list(first_tokens)[0]
                else:
                    # fallback: pick the first (works for 旭川: バナナ館 / 管理本館事務室)
                    facility_name = facility_candidates[0]

        # Post-trim facility
        facility_name = _clean_common(facility_name)
        # Remove tail like "...入口チケット窓口"
        facility_name = re.sub(r"(入口)?(チケット)?窓口\s*$", "", facility_name).strip()
        # Remove quoted shop name suffix: 「...」
        facility_name = re.sub(r"「[^」]*」\s*$", "", facility_name).strip()
        # If it is a concatenated "公社下水道部", prefer the legal entity name ending at "公社"
        if "公社" in facility_name and " " not in facility_name and "　" not in facility_name:
            if facility_name.endswith("下水道部") or facility_name.endswith("下水道課") or facility_name.endswith("下水道局"):
                facility_name = facility_name.split("公社", 1)[0] + "公社"

        if not facility_name:
            return None

        # 4) Address line (can be empty)
        address_line = ""
        if addr_idx >= 0:
            address_line = _clean_common(lines[addr_idx])
            # cut after full-width or half-width space (often floor/building info)
            if "　" in address_line:
                address_line = address_line.split("　", 1)[0].strip()
            if " " in address_line:
                address_line = address_line.split(" ", 1)[0].strip()
            address_line = re.sub(r"^(平日|休日)[：:]\s*", "", address_line).strip()

        return facility_name, address_line

    @staticmethod
    def _split_location_blocks(location: str) -> List[str]:
        """
        Split a raw location text into logical blocks.

        Heuristic:
        - Many cards list multiple distribution places as:
          <facility/address lines>
          電話:...
          <next facility/address lines>
          電話:...
          ...
        - We keep the phone line inside each block and start a new block after it.
        - If there is no phone line at all, we keep the whole text as a single block.
        """
        if not location:
            return []

        lines = location.split("\n")
        blocks: List[str] = []
        current: List[str] = []

        for line in lines:
            current.append(line)
            lower = line.lower()
            if "電話" in line or "tel" in lower:
                blocks.append("\n".join(current))
                current = []

        if current:
            blocks.append("\n".join(current))

        # Drop blocks that are clearly just 問合せ先／問い合わせ先 情報（住所を持たない問い合わせ先）
        def _is_inquiry_block(text: str) -> bool:
            ls = [l.strip() for l in text.split("\n") if l.strip()]
            if not ls:
                return False
            first = ls[0]
            if first.startswith("（問合せ先") or first.startswith("(問合せ先"):
                return True
            if first.startswith("（問い合わせ先") or first.startswith("(問い合わせ先"):
                return True
            if first.startswith("（問合せ") or first.startswith("(問合せ"):
                return True
            if first.startswith("（問い合わせ") or first.startswith("(問い合わせ"):
                return True
            return False

        filtered_blocks = [b for b in blocks if not _is_inquiry_block(b)]

        # Fallback: if we somehow produced no block but had input, keep original.
        if not filtered_blocks and location.strip():
            return [location]
        return filtered_blocks

    @classmethod
    def _parse_locations(cls, location: str, prefecture_name: str) -> List[Tuple[str, str]]:
        """
        Parse one location field into zero, one or multiple (facility, address) pairs.

        For backward compatibility, this reuses the existing _parse_location logic on
        each split block. If no block yields a result, we fall back to trying the
        whole string once (so existing single-location cases behave as before).
        """
        if not location:
            return []

        results: List[Tuple[str, str]] = []
        blocks = cls._split_location_blocks(location)
        for block in blocks:
            parsed = cls._parse_location(block, prefecture_name)
            if parsed:
                results.append(parsed)

        # We only care about entries that have a non-empty address (i.e. can become Facility).
        results = [(name, addr) for (name, addr) in results if addr]
        if results:
            return results

        # Fallback: try the whole text as one block (old behavior).
        single = cls._parse_location(location, prefecture_name)
        return [single] if single else []

    @staticmethod
    def _detect_city_id_from_address(
        address: str,
        pref_id: int,
        cities_by_pref: Dict[int, List[Tuple[str, int]]],
    ) -> Optional[int]:
        if not address or not pref_id:
            return None

        candidates = cities_by_pref.get(pref_id) or []
        for name, city_id in candidates:
            if name and name in address:
                return city_id
        return None

    def _upsert_facility(
        self,
        facility_name: str,
        address: str,
        pref_id: int,
        city_id: Optional[int],
    ) -> Optional[Facility]:
        if not facility_name or not pref_id:
            return None

        existing = Facility.get_by_name_and_pref(facility_name, pref_id)
        if existing:
            facility = existing
        else:
            facility = Facility(name=facility_name, pref_id=pref_id)

        facility.type = Facility.FacilityType.MANHOLE_CARD.value
        facility.address = address
        facility.postcode = None
        facility.latitude = None
        facility.longtitude = None
        facility.business_hours = None
        facility.pref_id = pref_id
        facility.city_id = city_id

        success = facility.save()
        if not success:
            logging.error(f"Failed to save Facility for {facility_name}")
            return None
        return facility

    def _upsert_manhole_card(self, pref_id: int, record: dict) -> Optional[ManholeCard]:
        name = (record.get("city") or "").strip()
        series = (record.get("series") or "").strip()
        release_date = (record.get("release_date") or "").strip()
        location_info = (record.get("location") or "").strip()
        distribution_time = (record.get("distribution_time") or "").strip()
        image_url = (record.get("image") or "").strip()

        if not name or not series:
            return None

        existing = ManholeCard.get_by_name_and_series(name, series)
        if existing:
            card = existing
        else:
            card = ManholeCard(name=name, series=series)

        card.release_date = release_date
        card.location_info = location_info
        card.distribution_time = distribution_time
        card.image_url = image_url
        card.pref_id = pref_id

        success = card.save()
        if not success:
            logging.error(f"Failed to save ManholeCard for {name} ({series}, {release_date})")
            return None
        return card

    @staticmethod
    def _link_card_facility(card_id: int, facility_id: int) -> None:
        if not card_id or not facility_id:
            return

        existing = ManholeCardFacility.get_by_fuzzy_id(card_id, facility_id)
        if existing:
            return

        link = ManholeCardFacility(manhole_card_id=card_id, facility_id=facility_id)
        success = link.save()
        if not success:
            logging.error(f"Failed to link ManholeCard(id={card_id}) with Facility(id={facility_id})")

    def start(self):
        root = TMP_ROOT / "manhole_card"
        if not root.exists():
            logging.error(f"TMP_ROOT/manhole_card directory not found: {root}")
            return self.FAILURE

        prefectures = self._load_prefectures()
        cities_by_pref = self._load_cities_by_pref()

        changed = False
        unparsed_locations: List[dict] = []

        for pref_dir in root.iterdir():
            if not pref_dir.is_dir():
                continue

            data_file = Path(pref_dir) / "data.json"
            if not data_file.exists():
                continue

            key = pref_dir.name
            prefecture = prefectures.get(key)
            if not prefecture:
                logging.warning(f"Skip directory {key}: prefecture not found in DB")
                continue

            pref_id = prefecture.pref_id

            try:
                with open(data_file, "r", encoding="utf-8") as f:
                    records = json.load(f)
            except (OSError, json.JSONDecodeError) as e:
                logging.error(f"Failed to load data from {data_file}: {e}")
                continue

            logging.info(f"Migrating ManholeCard data for prefecture {key} from {data_file}")

            for r in records:
                # Always insert/update ManholeCard first; facility/linking is best-effort.
                card = self._upsert_manhole_card(pref_id, r)
                if card:
                    changed = True

                location = r.get("location") or ""
                parsed_list = self._parse_locations(location, prefecture.full_name)
                if not parsed_list:
                    unparsed_locations.append(
                        {
                            "prefecture_en": key,
                            "prefecture_ja": prefecture.full_name,
                            "city": r.get("city"),
                            "series": r.get("series"),
                            "release_date": r.get("release_date"),
                            "location": location,
                            "reason": "parse_failed",
                        }
                    )
                    continue

                for facility_name, address in parsed_list:
                    city_id = self._detect_city_id_from_address(address, pref_id, cities_by_pref)

                    facility = self._upsert_facility(facility_name, address, pref_id, city_id)
                    if not facility or not facility.id:
                        continue

                    if not card or not card.id:
                        continue

                    self._link_card_facility(card.id, facility.id)
                    changed = True

        if unparsed_locations:
            report_path = root / "migration_report.json"
            try:
                with open(report_path, "w", encoding="utf-8") as f:
                    json.dump(unparsed_locations, f, ensure_ascii=False, indent=2)
                logging.info(f"Unparsed locations report written to {report_path}")
            except OSError as e:
                logging.error(f"Failed to write migration report: {e}")

        if changed:
            return self.SUCCESS
        else:
            return self.NO_WORK_TO_DO
