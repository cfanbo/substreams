package v8

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/streamingfast/substreams/wasm"
	"google.golang.org/protobuf/proto"
	"rogchap.com/v8go"
)

//go:embed runtime/polyfill.bundle.js
var polyfillCode string

type V8Module struct {
	iso      *v8go.Isolate
	code     []byte
	registry *wasm.Registry
}

func (mod *V8Module) NewInstance(context.Context) (wasm.Instance, error) {
	return NewV8Instance(mod.iso)
}

func (mod *V8Module) ExecuteNewCall(
	ctx context.Context,
	call *wasm.Call,
	cachedInstance wasm.Instance,
	_ []wasm.Argument,
	argValues map[string][]byte,
) (wasm.Instance, error) {

	inst := getInstance(mod.iso, cachedInstance)

	// Used to inject input, entry buffer
	var inputs []*v8go.Value
	for _, val := range argValues {
		inputsVal, err := v8go.NewUint8Array(inst.ctx, val)
		if err != nil {
			inst.Close(ctx)
			return nil, fmt.Errorf("creating Uint8Array from input: %w", err)
		}
		inputs = append(inputs, inputsVal)
	}

	// Runs all scripts (will be changed depending on files needed), probably going to merge all that are needed. This if makes sure we load our needed scripts ONLY on the first call
	if cachedInstance == nil {

		if err := injectAllGlobals(inst.ctx, call); err != nil {
			inst.Close(ctx)
			return nil, err
		}

		scripts := []struct{ code, name string }{
			{polyfillCode, "polyfill.js"},
			{string(mod.code), "bundle.js"},
		}
		for _, s := range scripts {
			if err := runJS(inst, s.code, s.name); err != nil {
				inst.Close(ctx)
				return nil, err
			}
		}
	}

	// call the handlers from JS side
	if err := callHandlers(inst, call, inputs); err != nil {
		inst.Close(ctx)
		return nil, err
	}

	outBytes, err := getOutput(inst)
	if err != nil {
		inst.Close(ctx)
		return nil, err
	}

	call.SetReturnValue(outBytes)
	return inst, nil
}

func injectAllGlobals(ctx *v8go.Context, call *wasm.Call) error {

	if err := injectStoreFunction(ctx, call); err != nil {
		return fmt.Errorf("injectStoreFunction: %w", err)
	}

	if err := injectClockFunction(ctx, call); err != nil {
		return fmt.Errorf("injectClockFunction: %w", err)
	}

	return nil
}

func (mod *V8Module) Close(context.Context) error {
	mod.iso.Dispose()
	return nil
}

func getInstance(iso *v8go.Isolate, cachedInstance wasm.Instance) *V8Instance {
	if cachedInstance != nil {
		return cachedInstance.(*V8Instance)
	}
	v8, _ := NewV8Instance(iso)
	return v8
}

func runJS(inst *V8Instance, code, filename string) error {
	if _, err := inst.ctx.RunScript(code, filename); err != nil {
		return fmt.Errorf("executing %s: %w", filename, err)
	}
	return nil
}

// callHandlers determines the type of handler (map, store) for a given module name and dispatches the execution to the appropriate handler function.
func callHandlers(inst *V8Instance, call *wasm.Call, inputs []*v8go.Value) error {
	handlerName := call.ModuleName

	// Check the type of handler (map, store)
	typeCheckVal, err := inst.ctx.Global().Get("getHandlerType")
	if err != nil {
		return fmt.Errorf("missing getHandlerType: %w", err)
	}
	getHandlerType, err := typeCheckVal.AsFunction()
	if err != nil {
		return fmt.Errorf("getHandlerType not a function: %w", err)
	}
	nameVal, _ := v8go.NewValue(inst.ctx.Isolate(), handlerName)
	handlerTypeVal, err := getHandlerType.Call(v8go.Undefined(inst.ctx.Isolate()), nameVal)
	if err != nil {
		return fmt.Errorf("getHandlerType call failed: %w", err)
	}

	// Dispatch to the appropriate handler
	switch handlerTypeVal.String() {
	case "map":
		return callMapHandler(inst, handlerName, inputs)
	case "store":
		return callStoreHandler(inst, handlerName, inputs)
	default:
		return fmt.Errorf("unknown handler type: %s", handlerTypeVal.String())
	}
}

// Executes a map handler registered in the JS context.
func callMapHandler(inst *V8Instance, handlerName string, inputs []*v8go.Value) error {
	handlerVal, err := inst.ctx.Global().Get("executeMapHandler")
	if err != nil {
		return fmt.Errorf("could not get executeMapHandler: %w", err)
	}
	handler, err := handlerVal.AsFunction()
	if err != nil {
		return fmt.Errorf("executeMapHandler is not a function: %w", err)
	}

	nameVal, err := v8go.NewValue(inst.ctx.Isolate(), handlerName)
	if err != nil {
		return fmt.Errorf("failed to create name string value: %w", err)
	}

	// Convert to []v8go.Valuer
	vals := make([]v8go.Valuer, 0, len(inputs)+1)
	vals = append(vals, nameVal)
	for _, v := range inputs {
		vals = append(vals, v)
	}

	result, err := handler.Call(v8go.Undefined(inst.ctx.Isolate()), vals...)
	if err != nil {
		return fmt.Errorf("failed to execute map handler: %w", err)
	}

	// If no result was returned, skip
	if result.IsNull() || result.IsUndefined() {
		return nil
	}

	if !result.IsUint8Array() {
		return fmt.Errorf("map handler did not return a Uint8Array")
	}

	// Save to instance so it can be retrieved
	inst.output = result.Uint8Array()
	return nil
}

// Executes a store handler registered in the JS context.
func callStoreHandler(inst *V8Instance, handlerName string, inputs []*v8go.Value) error {
	storeFuncVal, err := inst.ctx.Global().Get("executeStoreHandler")
	if err != nil {
		return fmt.Errorf("could not get executeStoreHandler: %w", err)
	}
	storeFunc, err := storeFuncVal.AsFunction()
	if err != nil {
		return fmt.Errorf("executeStoreHandler is not a function: %w", err)
	}

	nameVal, _ := v8go.NewValue(inst.ctx.Isolate(), handlerName)

	// Get the global store interface defined in the SDK
	storeIface, err := inst.ctx.Global().Get("__store_interface")
	if err != nil {
		return fmt.Errorf("could not get __store_interface: %w", err)
	}

	vals := []v8go.Valuer{nameVal, storeIface}
	for _, v := range inputs {
		vals = append(vals, v)
	}

	_, err = storeFunc.Call(v8go.Undefined(inst.ctx.Isolate()), vals...)
	return err
}

func getOutput(inst *V8Instance) ([]byte, error) {
	// No output
	if inst.output == nil {
		return nil, nil
	}
	return inst.output, nil
}

// Injects __clock into runtime
func injectClockFunction(ctx *v8go.Context, call *wasm.Call) error {
	iso := ctx.Isolate()

	clockFunc := v8go.NewFunctionTemplate(iso, func(info *v8go.FunctionCallbackInfo) *v8go.Value {
		clock := call.Clock

		data, err := proto.Marshal(clock)
		if err != nil {
			panic(fmt.Errorf("marshal clock: %w", err))
		}

		clockVal, err := v8go.NewUint8Array(ctx, data)
		if err != nil {
			panic(fmt.Errorf("clock Uint8Array: %w", err))
		}

		return clockVal
	})

	clockInstance := clockFunc.GetFunction(ctx)
	return ctx.Global().Set("__clock", clockInstance)
}

func injectStoreFunction(ctx *v8go.Context, call *wasm.Call) error {
	iso := ctx.Isolate()

	storeSetFunc := v8go.NewFunctionTemplate(iso, func(info *v8go.FunctionCallbackInfo) *v8go.Value {
		if len(info.Args()) != 3 {
			panic("__store_set expects 3 arguments")
		}

		ordinal := info.Args()[0].Integer()
		key := info.Args()[1].String()
		value := info.Args()[2]

		if !value.IsUint8Array() {
			panic("__store_set expects a Uint8Array as value")
		}

		data := value.Uint8Array()
		call.DoSetIfNotExists(uint64(ordinal), key, data)
		return nil
	})

	storeSetInstance := storeSetFunc.GetFunction(ctx)
	return ctx.Global().Set("__store_set", storeSetInstance)
}
