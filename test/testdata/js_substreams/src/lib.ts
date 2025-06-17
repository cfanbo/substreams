import { substreams, getClock, Store } from "@substreams/sdk";
import { create } from "@bufbuild/protobuf";
import { Block, BlockSchema, MapResultSchema } from "./pb/sf/substreams/v1/test/test_pb";
import { StoreDelta, StoreDeltaSchema, StoreDeltas, StoreDeltasSchema } from "./pb/sf/substreams/v1/deltas_pb";
export default class Substreams {
	@substreams.handlers.map([BlockSchema], MapResultSchema)
	js_test_map(blk: Block) {
		return create(MapResultSchema, {
			blockNumber: blk.number,
			blockHash: blk.id,
		});
	}
	@substreams.handlers.map([BlockSchema], MapResultSchema)
	js_test_map_clock() {
		const clock = getClock();
		return create(MapResultSchema, {
			blockNumber: clock.number,
			blockHash: clock.id,
		});
	}

	@substreams.handlers.store([BlockSchema], BlockSchema)
	js_store_source_key(store: Store<any>, blk: Block) {
		const key = `block_${blk.number}`;
		store.set(0, key, 0n);
	}
}
