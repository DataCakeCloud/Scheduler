# -*- coding: utf-8 -*-
# !/usr/bin/env python


# task_type
PIPELINE = "pipeline"
FIRST = "firstPipeline"
BACKFILL = 'backCalculation'
CLEAR = 'clear_backCalculation'
MANUALLY = 'manually'

# 任务例行和任务补数使用不同的并发数
BACKFILL_QUEUE = [BACKFILL,CLEAR]
PIPELINE_QUEUE = [FIRST,PIPELINE]