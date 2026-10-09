"""``python -m mlbench <command>``

export     read-only export from production (stdlib only; run on the DB host)
build      raw CSV export -> typed Parquet + 15-min truth + covariates
run        rolling-origin backtest (baselines + plug-in models)
report     bootstrap CIs, horizon curves, hand-over table, decision metrics, summary.md
gpu-check  print torch / CUDA / arch list (sm_120 needed for the RTX 5080)
"""

from __future__ import annotations

import argparse
import sys


def gpu_check() -> int:
    import torch

    print("torch", torch.__version__, "cuda", torch.version.cuda)
    print("cuda available:", torch.cuda.is_available())
    print("arch list:", torch.cuda.get_arch_list())
    if torch.cuda.is_available():
        print("device:", torch.cuda.get_device_name(0), torch.cuda.get_device_capability(0))
        x = torch.randn(1024, 1024, device="cuda")
        print("matmul ok:", float((x @ x).sum().item()) == float((x @ x).sum().item()))
    return 0 if "sm_120" in torch.cuda.get_arch_list() else 1


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="mlbench", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    from . import export

    export.add_args(sub.add_parser("export", help="read-only production export"))
    sp = sub.add_parser("build", help="raw export -> parquet + truth")
    sp.add_argument("--export", required=True)
    sp.add_argument("--memory", default="6GB")
    sp.add_argument("--threads", type=int, default=8)
    sp.add_argument("--ml-service-dir", default=None)
    sp = sub.add_parser("run", help="rolling-origin backtest")
    from .runner import add_args as run_args

    run_args(sp)
    sp = sub.add_parser("report", help="score a run directory")
    from .report import add_args as report_args

    report_args(sp)
    sub.add_parser("gpu-check", help="verify torch sees the GPU with sm_120 kernels")
    args = p.parse_args(argv)
    if args.cmd == "export":
        export.run_export(args)
        return 0
    if args.cmd == "build":
        from .build import main as build_main

        return build_main(args)
    if args.cmd == "run":
        from .runner import main as run_main

        return run_main(args)
    if args.cmd == "report":
        from .report import main as report_main

        return report_main(args)
    if args.cmd == "gpu-check":
        return gpu_check()
    return 2


if __name__ == "__main__":
    sys.exit(main())
