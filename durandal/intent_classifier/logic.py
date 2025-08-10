import asyncio
import logging

from orchestrator.conf import conf, processors

logger = logging.getLogger(conf.APP_NAME)



# async def process_task(task: TaskIn) -> None:
#     task_result = {
#         "task_id": str(task.task_id),
#         "status": "processing",
#         "timestamp": asyncio.get_event_loop().time(),
#         "is_partial": task.is_partial,
#     }
    
#     logger.info(
#         "Processing task %r",
#         task
#     )

#     try:
#         # Find out how many intents to classify and their borders
#         segments = await segment_text(processors.INTENT_SEPARATORS, task.content)

#         # Run segments through classification layers
#         layers = (
#             processors.CLASSIFICATION_LAYERS
#             if task.priority_order == "ascending"
#             else reversed(processors.CLASSIFICATION_LAYERS)
#         )
#         running_cost = 0
#         classification_results = []

#         for i in range(len(segments)):
#             segment = segments[i]
#             logger.info("Processing segment %d/%d: %r", i + 1, len(segments), segment)
#             logger.debug("Current running cost: %d", running_cost)

#             running_cost, result = await classify_segment(
#                 task,
#                 segments[:i],
#                 segment,
#                 layers,
#                 running_cost=running_cost,
#                 is_partial=task.is_partial,
#             )
#             logger.debug("Segment %d result: %r, updated running cost: %d", i + 1, result, running_cost)
#             if result is None:
#                 logger.info("No classification result for segment %d", i + 1)
#                 continue

#             classification_results.append(
#                 {
#                     "segment": segment,
#                     "eval": result,
#                 }
#             )

#         task_result["status"] = "completed"
#         task_result["results"] = classification_results
#         logger.debug("Task completed with results: %r", classification_results)

#     # Incase of any error, store the error result
#     except Exception as e:
#         logger.exception("Error processing task %s", task.task_id)
#         task_result["status"] = "failed"
#         task_result["error"] = str(e)
#         logger.debug("Task failed with error: %s", str(e))

#     finally:
#         # Store the result in the result store
#         logger.debug("Storing result for task %s: %r", task.task_id, task_result)
#         await result_store.store_result(task.task_id, task_result)
#         logger.info("Finished task %s and stored result", task.task_id)

#         # Let callback url know that its done if provided (failed or succeeded both)
#         if task.callback_url:
#             logger.info("Sending result to callback URL: %s", task.callback_url)
#             try:
#                 await send_post_request(task.callback_url, task_result)
#                 logger.debug("Callback POST succeeded for URL: %s", task.callback_url)
#             except httpx.RequestError as e:
#                 logger.exception("An error occurred while requesting: %s", e)
#             except httpx.HTTPStatusError as e:
#                 logger.exception(
#                     "Callback URL returned status %s", e.response.status_code
#                 )
#             except Exception as e:
#                 logger.exception("Callback POST failed: %s", e)


# --------------------------------------------------------------------------- #
# Layer logic
# --------------------------------------------------------------------------- #
async def classify_segment(
    task,
    previous_segments: list[str],
    segment: str,
    layers: list[dict],
    running_cost: int = 0,
    is_partial: bool = False,
) -> tuple[int, dict] | None:
    
    for layer in layers:
        layer_instance = layer["instance"]
        if layer.get("cost", 0) + running_cost > task.job_budget or not await layer_instance.check_condition(
            previous_segments, segment, is_partial=is_partial):
            continue

        result = await layer_instance.classify(
            previous_segments, segment, is_partial=is_partial
        )
        running_cost += layer.get("cost", 0)
        await layer_instance.on_complete(result)

        threshold = float(
            layer.get("confidence_threshold", processors.DEFAULT_INTENT_CONFIDENCE_THRESHOLD)
        )
        result_confidence = result.get("confidence", 0.5)

        if result_confidence > threshold:
            await layer_instance.on_success(result)
            return running_cost, result
        else:
            await layer_instance.on_failure(result, "Result below confidence threshold")
    return running_cost, None



async def segment_text(intent_separators: list, content: str) -> list[str]:
    for intent_separator in intent_separators:
        separator_instance = intent_separator["instance"]
        
        logger.debug("Checking intent separator: %s", intent_separator["alias"])
        if await separator_instance.check_condition(content):
            logger.debug("Intent separator %s matched condition", intent_separator["alias"])
            segments = await separator_instance.create_segments(content)
            logger.debug("Segments created: %r", segments)

            if (segments is not None and len(segments) > 0 and
                all(await asyncio.gather(*[separator_instance.validate_segment(seg) for seg in segments]))):
                logger.debug("Using segments from separator: %s", intent_separator["alias"])
                return segments
        
    logger.debug("No intent separators matched; using original content as single segment.")
    return [content]  # Default to single segment if no separators
