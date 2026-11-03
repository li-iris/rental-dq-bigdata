"""生成合成数据（不含任何真实信息），模拟实习中遇到的"脏数据"问题：

- 同一个地址有多种写法、拼写错误、个别地址无法解析
- 租约来自两个版本的工作簿 + 业务录入表，版本之间有重叠和冲突
- 日期格式混杂，部分日期只写了"日.月"没有年份
- 水费超过 90 天未开票、检查通知天数不够、一年例行检查次数过多

用法：
    python data_gen/generate_data.py --dt 2026-11-02            # 只有 warn，流程正常结束
    python data_gen/generate_data.py --dt 2026-11-02 --with-errors  # 额外注入 error，演示流程中止
"""
from __future__ import annotations

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

STREETS = [("Smith", "Street"), ("George", "Road"), ("Pitt", "Street"), ("King", "Avenue"),
           ("Marrickville", "Road"), ("Church", "Lane"), ("Victoria", "Parade"), ("Station", "Place")]
TYPOS = {"Smith": "Smtih", "George": "Goerge", "Marrickville": "Marikville"}
FIRST = ["Alex", "Sam", "Chris", "Jordan", "Taylor", "Morgan", "Casey", "Jamie", "Riley", "Drew"]
LAST = ["Chen", "Nguyen", "Smith", "Wang", "Brown", "Li", "Patel", "Kim", "Jones", "Zhang"]


def addr_variant(unit, num, street, suffix, rng, allow_typo=True):
    """同一地址随机生成不同写法。"""
    name = TYPOS.get(street, street) if (allow_typo and rng.random() < 0.08) else street
    suf = rng.choice([suffix, suffix[:2] if suffix == "Street" else {"Road": "Rd", "Avenue": "Ave", "Lane": "Ln",
                                                                       "Parade": "Pde", "Place": "Pl"}.get(suffix, suffix)])
    if unit is None:
        base = f"{num} {name} {suf}"
    else:
        base = rng.choice([f"Unit {unit}/{num} {name} {suf}", f"{unit}/{num} {name} {suf}",
                           f"U{unit}/{num} {name.lower()} {suf.lower()}.", f"Unit {unit}, {num} {name} {suf}"])
    if rng.random() < 0.15:
        base = f"{base}, NSW 2000"
    return base


def fmt_date(d: date, rng) -> str:
    return rng.choice([d.isoformat(), d.isoformat(), d.strftime("%d/%m/%Y")])


def write_csv(path: Path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", required=True, help="业务日期 yyyy-MM-dd")
    ap.add_argument("--with-errors", action="store_true")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    dt = date.fromisoformat(args.dt)
    out = ROOT / "data" / "landing" / f"dt={args.dt}"

    # ---------- 房产 ----------
    props = []
    for i in range(30):
        street, suffix = STREETS[i % len(STREETS)]
        num = 10 + i * 3
        unit = None if i % 4 == 0 else (i % 6) + 1
        props.append((unit, num, street, suffix))
    prop_rows = []
    for i, p in enumerate(props):
        managed = "N" if i in (27, 28, 29) else "Y"
        prop_rows.append([addr_variant(*p, rng, allow_typo=False), managed, str(rng.choice([1, 2, 3]))])
    write_csv(out / "property.csv", ["property_address", "in_management", "bedrooms"], prop_rows)

    managed_props = props[:27]

    # ---------- 租约：v1、v2 两个版本 + 业务录入表 ----------
    v1, tenancies = [], []
    for i in range(40):
        p = managed_props[i % len(managed_props)]
        start = dt - timedelta(days=rng.randint(60, 600))
        end = start + timedelta(days=rng.choice([180, 365, 365]))
        if rng.random() < 0.3:
            end = None  # 周期租约，无到期日
        rent = rng.choice([320, 350, 380, 450, 620, 680, 750, 820])
        tid = f"T{1000 + i}"
        tenancies.append((tid, p, start, end, rent))
        v1.append([tid, addr_variant(*p, rng), f"{rng.choice(FIRST)} {rng.choice(LAST)}",
                   fmt_date(start, rng), fmt_date(end, rng) if end else "", rng.choice([str(rent), f"${rent}"])])
    # 一个无法解析的地址
    v1.append(["T1999", "behind the corner shop", "Pat Lee", fmt_date(dt - timedelta(days=100), rng), "", "300"])

    v2 = []
    for tid, p, start, end, rent in tenancies[:15]:
        new_rent = rent + rng.choice([0, 20, 30])  # v2 是更新后的版本，租金可能变了
        v2.append([tid, addr_variant(*p, rng), "", fmt_date(start, rng), fmt_date(end, rng) if end else "", str(new_rent)])
    for j in range(5):
        p = managed_props[(j * 5 + 2) % len(managed_props)]
        start = dt - timedelta(days=rng.randint(10, 50))
        v2.append([f"T2{j:03d}", addr_variant(*p, rng), f"{rng.choice(FIRST)} {rng.choice(LAST)}",
                   fmt_date(start, rng), "", str(rng.choice([330, 360, 700]))])
    # 一份租约租金漏填 -> 本月口径说不清（W04）
    p = managed_props[26]
    v2.append(["T2999", addr_variant(*p, rng), "Kai Wong", fmt_date(dt - timedelta(days=20), rng), "", ""])

    entry = [  # 业务方直接录入的更正，优先级最高
        [tenancies[0][0], addr_variant(*tenancies[0][1], rng, allow_typo=False), "", fmt_date(tenancies[0][2], rng), "", "700"],
        [tenancies[3][0], addr_variant(*tenancies[3][1], rng, allow_typo=False), "", fmt_date(tenancies[3][2], rng),
         fmt_date(dt + timedelta(days=90), rng), "480"],
    ]

    if args.with_errors:
        p = managed_props[5]
        v2.append(["T3001", addr_variant(*p, rng), "Err One", "2026-05-01", "2026-03-01", "500"])        # E02 期间倒置
        v2.append(["T3002", addr_variant(*p, rng), "Err Two", "2026-05-01", "", "-100"])                # E04 负租金
        v2.append(["T3003", addr_variant(*props[28], rng), "Err Three", "2026-05-01", "", "550"])       # E03 非在管房产

    header = ["tenancy_id", "property_address", "tenant_name", "start_date", "end_date", "weekly_rent"]
    write_csv(out / "tenancy_v1.csv", header, v1)
    write_csv(out / "tenancy_v2.csv", header, v2)
    write_csv(out / "tenancy_entry.csv", header, entry)

    # ---------- 水费：每季度一张 ----------
    wb, n = [], 0
    for i, p in enumerate(managed_props):
        for q in range(4):
            period_end = dt - timedelta(days=30 + q * 91)
            amount = round(rng.uniform(120, 420), 2)
            if q == 0:
                invoiced = ""  # 最近一季还没开票，正常
            elif rng.random() < 0.12:
                invoiced = ""  # 超过 90 天没开票 -> W01
            else:
                invoiced = fmt_date(period_end + timedelta(days=rng.randint(5, 40)), rng)
            n += 1
            wb.append([f"W{n:05d}", addr_variant(*p, rng), fmt_date(period_end, rng),
                       rng.choice([f"{amount}", f"${amount:,.2f}"]), invoiced])
    if args.with_errors:
        wb.append(["W90001", addr_variant(*managed_props[1], rng), fmt_date(dt - timedelta(days=60), rng), "-50", ""])  # E05
        wb.append(list(wb[0]))  # E06 账单重复
    write_csv(out / "water_bill.csv", ["bill_id", "property_address", "period_end", "amount", "invoiced_date"], wb)

    # ---------- 房屋检查 ----------
    ins, n = [], 0
    for i, p in enumerate(managed_props):
        times = 5 if i == 7 else rng.choice([2, 3, 4])  # 第 8 套房一年 5 次 -> W03
        for k in range(times):
            d = date(dt.year, 1, 15) + timedelta(days=k * 60 + rng.randint(0, 10))
            if d > dt:
                d = dt - timedelta(days=rng.randint(1, 30))
            gap = rng.choice([7, 10, 14, 14, 21])
            if rng.random() < 0.06:
                gap = rng.choice([3, 5])  # 通知不足 7 天 -> W02
            notice = d - timedelta(days=gap)
            # 部分通知日期只写了 日.月
            notice_s = f"{notice.day}.{notice.month}" if rng.random() < 0.3 else fmt_date(notice, rng)
            n += 1
            ins.append([f"I{n:05d}", addr_variant(*p, rng), "routine", fmt_date(d, rng), notice_s])
    ins.append([f"I{n + 1:05d}", addr_variant(*managed_props[2], rng), "entry", fmt_date(dt - timedelta(days=15), rng), "next week"])  # W06
    write_csv(out / "inspection.csv", ["inspection_id", "property_address", "inspection_type", "inspection_date", "notice_date"], ins)

    print(f"合成数据已生成：{out}")
    for f in sorted(out.glob("*.csv")):
        print(f"  {f.name}: {sum(1 for _ in open(f, encoding='utf-8')) - 1} 行")


if __name__ == "__main__":
    main()
