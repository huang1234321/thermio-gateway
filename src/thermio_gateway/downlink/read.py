"""down/read 自检响应（gateway.md §5.3/§7.2；M2-import.md §9.2）。

``down/read {job_id, req_id, points[]}`` → 对清单点**立即一次性补采**
（插队到下一调度槽，优先级高于常规周期）→ 结果走**正常 up/data**
（自检样本与生产样本同管线，无旁路；M2 §9.2 既定语义）。

- 补采失败（超时/错误）的点同样上行（``value=null, quality=bad``）——
  自检命中率口径包含「采不到」的诚实暴露；
- 批次 ≤500 由云端保证（M2 §9.2），本地同样防御性切分；
- 无专用 ack topic（响应即正常数据上行）。
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from ..contracts import SelfCheckRead
from ..pointmap import PointRow

log = logging.getLogger("gw.down.read")

# ingest.md §3.2 单消息上限 500 点（防御性切分，云端已保证）
READ_BATCH_LIMIT = 500


def split_read_batches(
    msg: SelfCheckRead, lookup: Callable[[str], PointRow | None]
) -> list[list[PointRow]]:
    """自检清单 → 可采点分批（≤500/批）。清单点不在 pointmap/enabled → bad 行
    由补采周期以「查无地址」落 quality=bad——与 §7.2 诚实暴露口径一致。
    """
    batches: list[list[PointRow]] = []
    current: list[PointRow] = []
    for raw_name in msg.points:
        point = lookup(raw_name)
        if point is None:
            log.warning("down/read 引用未知点 %s（bad 上行）", raw_name)
            continue
        current.append(point)
        if len(current) >= READ_BATCH_LIMIT:
            batches.append(current)
            current = []
    if current:
        batches.append(current)
    return batches
