module Events

import ..TimespanLogging: Event, init_similar

"""
    CoreMetrics

Tracks the timestamp, category, and kind of the `Event` object generated by log
events.
"""
struct CoreMetrics end

(::CoreMetrics)(ev::Event{T}) where T =
    (;timestamp=ev.timestamp,
      category=ev.category,
      kind=T)

"""
    IDMetrics

Tracks the ID of `Event` objects generated by log events.
"""
struct IDMetrics end

(::IDMetrics)(ev::Event{T}) where T = ev.id

"""
    TimelineMetrics

Tracks the timeline of `Event` objects generated by log events.
"""
struct TimelineMetrics end

(::TimelineMetrics)(ev::Event{T}) where T = ev.timeline

"""
    FullMetrics

Tracks the full `Event` object generated by log events.
"""
struct FullMetrics end

(::FullMetrics)(ev) = ev

"""
    CPULoadAverages

Monitors the CPU load averages.
"""
struct CPULoadAverages end

(::CPULoadAverages)(ev::Event) = Sys.loadavg()[1]

"""
    MemoryFree

Monitors the percentage of free system memory.
"""
struct MemoryFree end

(::MemoryFree)(ev::Event) = Sys.free_memory() / Sys.total_memory()

"""
    EventSaturation

Tracks the compute saturation (running tasks) per-processor.
"""
mutable struct EventSaturation
    saturation::Dict{Symbol,Int}
end
EventSaturation() = EventSaturation(Dict{Symbol,Int}())
init_similar(::EventSaturation) = EventSaturation()

function (es::EventSaturation)(ev::Event{:start})
    old = get(es.saturation, ev.category, 0)
    es.saturation[ev.category] = old + 1
    NamedTuple(filter(x->x[2]>0, es.saturation))
end
function (es::EventSaturation)(ev::Event{:finish})
    old = get(es.saturation, ev.category, 0)
    es.saturation[ev.category] = old - 1
    NamedTuple(filter(x->x[2]>0, es.saturation))
end

"Debugging metric, used to log event start/finish via `@debug`."
struct DebugMetrics
    sat::EventSaturation
end
DebugMetrics() = DebugMetrics(EventSaturation())
function (dm::DebugMetrics)(ev::Event{T}) where T
    dm.sat(ev)
    sat_string = "$(join(["$cat => $(dm.sat.saturation[cat])" for cat in keys(dm.sat.saturation) if dm.sat.saturation[cat] > 0 ], ", "))"
    @debug "Event: $(ev.category) $T ($sat_string)" _line=nothing _file=nothing _module=nothing
    nothing
end
init_similar(dm::DebugMetrics) = DebugMetrics()

"""
    LogWindow

Aggregator that prunes events to within a given time window.
"""
mutable struct LogWindow
    window_length::UInt64
    core_name::Symbol
    creation_handlers::Vector{Any}
    deletion_handlers::Vector{Any}
end
LogWindow(window_length, core_name) = LogWindow(window_length, core_name, [], [])

function (lw::LogWindow)(logs::Dict)
    core_log = logs[lw.core_name]

    # Inform creation hooks
    if length(core_log) > 0
        log = Dict{Symbol,Any}()
        for key in keys(logs)
            log[key] = last(logs[key])
        end
        for obj in lw.creation_handlers
            creation_hook(obj, log)
        end
    end

    # Find entries outside the window
    window_start = time_ns() - lw.window_length
    idx = something(findfirst(ev->ev.timestamp >= window_start, core_log),
                    length(core_log))

    # Return if all events are in the window
    if length(core_log) == 0 || (core_log[1].timestamp >= window_start)
        return
    end

    # Inform deletion hooks
    for obj in lw.deletion_handlers
        deletion_hook(obj, idx)
    end

    # Remove entries outside the window
    for name in keys(logs)
        deleteat!(logs[name], 1:idx)
    end
end

creation_hook(x, log) = nothing
deletion_hook(x, idx) = nothing

end # module Events
