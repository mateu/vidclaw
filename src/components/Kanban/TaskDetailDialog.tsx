import React, { useState, useEffect, useMemo, useRef } from 'react'
import { X, Bot, User, Activity, FileText, AlertCircle, Clock, CheckCircle2, Loader2, MessageCircle, Copy, Check } from 'lucide-react'
import AttachmentSection from './AttachmentSection'
import { cn } from '@/lib/utils'
import { extractFilePaths } from './TaskCard'
import { useTimezone } from '../TimezoneContext'
import { useNavigate } from '@tanstack/react-router'
import MarkdownRenderer from '../Markdown/MarkdownRenderer'
import { api } from '@/lib/api'
import { getTaskDisplayState, isErrorState, getDisplayMessage } from '@/lib/taskExecution'
import type { Task, Attachment, ActivityEntry } from '@/types/api'

function formatTime(iso: string | null | undefined, tz: string): string {
  if (!iso) return ''
  const d = new Date(iso)
  return d.toLocaleDateString('en-US', { timeZone: tz, month: 'short', day: 'numeric' }) + ' ' + d.toLocaleTimeString('en-US', { timeZone: tz, hour: 'numeric', minute: '2-digit', second: '2-digit' })
}

function formatDuration(startIso: string | null | undefined, endIso: string | null | undefined): string | null {
  if (!startIso || !endIso) return null
  const ms = new Date(endIso).getTime() - new Date(startIso).getTime()
  if (ms < 0) return null
  const secs = Math.floor(ms / 1000)
  if (secs < 60) return `${secs}s`
  const mins = Math.floor(secs / 60)
  if (mins < 60) return `${mins}m ${secs % 60}s`
  const hrs = Math.floor(mins / 60)
  return `${hrs}h ${mins % 60}m`
}

function formatTimeAgo(iso: string | null | undefined): string {
  if (!iso) return ''
  const d = new Date(iso)
  const now = new Date()
  const diff = now.getTime() - d.getTime()
  if (diff < 60000) return 'just now'
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) + ' ' + d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' })
}

const ACTION_LABELS: Record<string, string> = {
  task_created: 'Created task',
  task_updated: 'Updated task',
  task_run: 'Started task',
  task_pickup: 'Picked up task',
  task_completed: 'Completed task',
  task_cancelled: 'Cancelled task',
  task_deleted: 'Deleted task',
  task_status_check: 'Checked status',
  task_timeout: 'Timed out',
  task_archived: 'Archived task',
  schedule_toggled: 'Toggled schedule',
}

const STATUS_LABELS: Record<string, string> = {
  todo: 'To Do', 'in-progress': 'In Progress', review: 'Review', done: 'Done', blocked: 'Blocked',
}

interface ActivityDescribed {
  verb: string
  detail?: string
}

function describeActivity(a: CollapsedActivity): ActivityDescribed {
  const d = a.details || {}
  const label = ACTION_LABELS[a.action] || a.action

  if (a.action === 'task_updated' && d.newStatus) {
    return { verb: `Moved to ${STATUS_LABELS[d.newStatus as string] || d.newStatus}` }
  }
  if (a.action === 'task_updated' && d.newPriority) {
    return { verb: `Set priority to ${d.newPriority}` }
  }
  if (a.action === 'task_updated' && (d.changes as string[] | undefined)?.length) {
    const fields = (d.changes as string[]).filter((c: string) => c !== '_actor').join(', ')
    return { verb: `Updated ${fields}` }
  }
  if (a.action === 'schedule_toggled') {
    return { verb: d.enabled ? 'Enabled schedule' : 'Paused schedule' }
  }
  if (a.action === 'task_status_check' && d.status) {
    return { verb: `Status: ${d.status}`, detail: d.message as string | undefined }
  }
  if (a.action === 'task_completed') {
    return { verb: label, detail: (d.error || d.result) as string | undefined }
  }
  return { verb: label }
}

interface CollapsedActivity extends ActivityEntry {
  count?: number
  grouped?: boolean
  groupLabel?: string
}

function collapseActivities(activities: ActivityEntry[]): CollapsedActivity[] {
  if (!activities.length) return []
  const result: CollapsedActivity[] = []
  for (const a of activities) {
    const prev = result[result.length - 1]
    if (prev && !prev.grouped && !prev.details?.result && !prev.details?.error && !(prev.details?.message as string | undefined) &&
        prev.actor === a.actor && prev.action === a.action &&
        prev.details?.taskId === a.details?.taskId) {
      prev.count = (prev.count || 1) + 1
      prev.timestamp = a.timestamp
      continue
    }
    if (prev && !prev.grouped &&
        prev.actor === a.actor &&
        prev.details?.taskId === a.details?.taskId &&
        prev.action !== a.action) {
      const FLOW: Record<string, string> = { task_pickup: 'task_completed', task_run: 'task_completed' }
      if (FLOW[prev.action] === a.action) {
        result[result.length - 1] = {
          ...a, grouped: true,
          groupLabel: `${ACTION_LABELS[prev.action]} \u2192 ${ACTION_LABELS[a.action]}`,
        }
        continue
      }
    }
    result.push({ ...a })
  }
  return result
}

interface ActivityLogProps {
  taskId: string
}

function ActivityLog({ taskId }: ActivityLogProps) {
  const [activities, setActivities] = useState<ActivityEntry[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let mounted = true
    const load = () => {
      api.activity.list({ limit: 50, taskId })
        .then(data => { if (mounted) { setActivities(data); setLoading(false) } })
        .catch(() => { if (mounted) setLoading(false) })
    }
    load()
    return () => { mounted = false }
  }, [taskId])

  if (loading) return <div className="text-xs text-muted-foreground p-4">Loading activity...</div>
  if (!activities.length) return <div className="text-xs text-muted-foreground p-4">No activity yet</div>

  const collapsed = collapseActivities(activities)

  return (
    <div className="space-y-1 p-1">
      {collapsed.map(a => {
        const { verb, detail } = a.grouped
          ? { verb: a.groupLabel!, detail: (a.details?.result || a.details?.error) as string | undefined }
          : describeActivity(a)
        return (
          <div key={a.id} className="flex items-start gap-2 px-2 py-1.5 rounded-md hover:bg-secondary/50 transition-colors">
            <div className={`mt-0.5 shrink-0 rounded-full p-1 ${a.actor === 'bot' ? 'bg-purple-500/20 text-purple-400' : 'bg-blue-500/20 text-blue-400'}`}>
              {a.actor === 'bot' ? <Bot size={10} /> : <User size={10} />}
            </div>
            <div className="flex-1 min-w-0">
              <p className="text-xs">
                <span className={`font-medium ${a.actor === 'bot' ? 'text-purple-400' : 'text-blue-400'}`}>
                  {a.actor === 'bot' ? 'Bot' : 'User'}
                </span>
                {' '}
                <span className="text-muted-foreground">{verb}</span>
                {a.count && a.count > 1 && <span className="text-muted-foreground/70 text-[10px] ml-1">&times;{a.count}</span>}
                {a.details?.hasError as boolean && <span className="text-red-400"> (error)</span>}
              </p>
              {detail && (
                <p className={cn(
                  'text-[11px] mt-0.5 line-clamp-3 whitespace-pre-wrap',
                  a.details?.hasError || a.details?.error ? 'text-red-400/80' : 'text-foreground/60'
                )}>{detail}</p>
              )}
              <p className="text-[10px] text-muted-foreground">{formatTimeAgo(a.timestamp)}</p>
            </div>
          </div>
        )
      })}
    </div>
  )
}

interface TaskDetailDialogProps {
  open: boolean
  onClose: () => void
  task: Task | null
}

interface RunEntryView {
  id: string
  startedAt: string | null
  completedAt: string | null
  updatedAt: string | null
  status: 'queued' | 'in-progress' | 'completed' | 'failed' | 'canceled' | 'timeout'
  reason: string | null
  message: string | null
  result: string | null
  error: string | null
  subagentId: string | null
  sessionId: string | null
  active?: boolean
}

export default function TaskDetailDialog({ open, onClose, task }: TaskDetailDialogProps) {
  const { timezone } = useTimezone()
  const navigate = useNavigate()
  const [attachments, setAttachments] = useState<Attachment[]>([])
  const [attKey, setAttKey] = useState(0)
  const [copiedText, setCopiedText] = useState<string | null>(null)
  const outputRef = useRef<HTMLDivElement | null>(null)
  const historyRef = useRef<HTMLDivElement | null>(null)

  const refreshAttachments = () => {
    if (!task?.id) return
    api.tasks.attachments.list(task.id).then(atts => {
      if (Array.isArray(atts)) setAttachments(atts)
    }).catch(() => {})
    setAttKey(k => k + 1)
  }

  useEffect(() => {
    if (task) setAttachments(task.attachments || [])
    else setAttachments([])
  }, [task, open])

  const [elapsed, setElapsed] = useState('')
  useEffect(() => {
    if (!open || !task || task.status !== 'in-progress' || !task.startedAt) { setElapsed(''); return }
    const tick = () => setElapsed(formatDuration(task.startedAt, new Date().toISOString()) || '')
    tick()
    const id = setInterval(tick, 1000)
    return () => clearInterval(id)
  }, [open, task?.id, task?.status, task?.startedAt])

  useEffect(() => {
    if (!open) return
    const handleKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose() }
    document.addEventListener('keydown', handleKey)
    return () => document.removeEventListener('keydown', handleKey)
  }, [open, onClose])

  const copyToClipboard = async (label: string, text: string | null | undefined) => {
    if (!text) return
    try {
      await navigator.clipboard.writeText(text)
      setCopiedText(label)
      setTimeout(() => setCopiedText(null), 1500)
    } catch {}
  }

  const runEntries = useMemo<RunEntryView[]>(() => {
    if (!task) return []

    const transitionEntries = (task.executionTransitions || []).map((tr, idx) => ({
      id: `transition-${idx}`,
      startedAt: tr.at || null,
      completedAt: tr.at || null,
      updatedAt: tr.at || null,
      status: tr.to,
      reason: tr.reason || null,
      message: tr.message || null,
      result: null,
      error: null,
      subagentId: tr.subagentId || null,
      sessionId: tr.subagentId || null,
      active: tr.to === 'in-progress',
    }))

    if (transitionEntries.length) {
      return transitionEntries.sort((a, b) => {
        const ta = new Date(a.updatedAt || a.startedAt || 0).getTime()
        const tb = new Date(b.updatedAt || b.startedAt || 0).getTime()
        return tb - ta
      })
    }

    const history = (task.runHistory || []).map((run, idx) => ({
      id: `history-${idx}`,
      startedAt: run.startedAt || null,
      completedAt: run.completedAt || null,
      updatedAt: run.updatedAt || run.completedAt || null,
      status: run.status || (run.error ? 'failed' : 'completed'),
      reason: run.reason || null,
      message: run.message || run.error || run.result || null,
      result: run.result || null,
      error: run.error || null,
      subagentId: (run as { subagentId?: string | null }).subagentId || null,
      sessionId: (run as { sessionId?: string | null }).sessionId || null,
    }))

    if (task.status === 'in-progress' || task.startedAt || task.completedAt || task.result || task.error) {
      const current: RunEntryView = {
        id: 'current',
        startedAt: task.startedAt || null,
        completedAt: task.completedAt || null,
        updatedAt: task.updatedAt || null,
        status: task.status === 'in-progress' ? 'in-progress' : (task.error ? 'failed' : (task.completedAt ? 'completed' : 'queued')),
        reason: null,
        message: task.error || task.result || null,
        result: task.result || null,
        error: task.error || null,
        subagentId: task.subagentId || null,
        sessionId: task.subagentId || null,
        active: task.status === 'in-progress',
      }
      if (!history.length || task.status === 'in-progress' || (task.completedAt && history[history.length - 1]?.completedAt !== task.completedAt)) {
        history.push(current)
      }
    }

    return history.sort((a, b) => {
      const ta = new Date(a.completedAt || a.updatedAt || a.startedAt || 0).getTime()
      const tb = new Date(b.completedAt || b.updatedAt || b.startedAt || 0).getTime()
      return tb - ta
    })
  }, [task])

  if (!open || !task) return null

  const isDone = task.status === 'done'
  const isInProgress = task.status === 'in-progress'
  const displayState = getTaskDisplayState(task)
  const hasError = isErrorState(displayState) || (!displayState && !!task.error)
  const displayMessage = getDisplayMessage(task, displayState)
  const duration = formatDuration(task.startedAt || task.createdAt, task.completedAt)
  const filePaths = extractFilePaths(task.result)
  const skillsList = task.skills && task.skills.length ? task.skills : (task.skill ? [task.skill] : [])

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm" onClick={onClose}>
      <div
        className="bg-card border border-border w-full max-w-4xl flex flex-col shadow-2xl rounded-xl max-h-[85vh]"
        onClick={(e: React.MouseEvent) => e.stopPropagation()}
      >
        <div className="flex items-start justify-between px-5 py-4 border-b border-border shrink-0">
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2">
              {isInProgress && <Loader2 size={16} className="text-amber-400 animate-spin shrink-0" />}
              {isDone && !hasError && <CheckCircle2 size={16} className="text-green-500 shrink-0" />}
              {isDone && hasError && <AlertCircle size={16} className="text-red-400 shrink-0" />}
              <h2 className="text-lg font-semibold">{task.title}</h2>
            </div>
            {(skillsList.length > 0 || task.channel) && (
              <div className="flex items-center gap-3 mt-1.5 flex-wrap">
                {skillsList.map(sk => (
                  <span key={sk} className="text-[11px] px-2 py-0.5 rounded-full bg-orange-500/20 text-orange-400">{sk}</span>
                ))}
                {task.channel && (
                  <span className="inline-flex items-center gap-1 text-[11px] px-2 py-0.5 rounded-full bg-blue-500/20 text-blue-400">
                    <MessageCircle size={10} /> {task.channel}
                  </span>
                )}
              </div>
            )}
            {(task.startedAt || task.completedAt) && (
              <div className="flex items-center gap-2 text-[10px] text-muted-foreground mt-1.5">
                <span>Started {formatTime(task.startedAt || task.createdAt, timezone)}</span>
                {duration && <span className="text-green-400 font-medium flex items-center gap-0.5"><Clock size={10} />{duration}</span>}
                {isInProgress && elapsed && <span className="text-amber-400 font-medium flex items-center gap-0.5"><Clock size={10} />{elapsed}</span>}
              </div>
            )}
            {task.source && (
              <div className="flex items-center gap-1.5 text-[11px] text-muted-foreground mt-1">
                <MessageCircle size={12} className="text-blue-400" />
                <span>Source: <span className="text-blue-400 font-medium">{task.source}</span></span>
                {task.sourceMessageId && <span className="text-muted-foreground/60">#{task.sourceMessageId}</span>}
              </div>
            )}
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground shrink-0 ml-3"><X size={18} /></button>
        </div>

        <div className="flex-1 overflow-hidden min-h-[300px] flex flex-col md:flex-row">
          <div className="w-full md:w-2/3 overflow-y-auto p-5 space-y-4">
            <div className="rounded-lg border border-border bg-secondary/20 p-3 space-y-3">
              <div className="flex items-center justify-between gap-2">
                <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Run Details</h3>
                <div className="flex items-center gap-1.5">
                  {displayMessage ? (
                    <button
                      onClick={() => outputRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })}
                      className="text-[11px] px-2 py-1 rounded bg-secondary hover:bg-accent transition-colors"
                    >
                      Jump to output
                    </button>
                  ) : null}
                  <button
                    onClick={() => historyRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })}
                    className="text-[11px] px-2 py-1 rounded bg-secondary hover:bg-accent transition-colors"
                  >
                    Jump to activity
                  </button>
                </div>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 text-[11px]">
                <div className="rounded border border-border/70 bg-background/30 px-2 py-1.5">
                  <span className="text-muted-foreground">Status</span>
                  <div className="font-medium mt-0.5">{displayState || task.status}</div>
                </div>
                <div className="rounded border border-border/70 bg-background/30 px-2 py-1.5">
                  <span className="text-muted-foreground">Updated</span>
                  <div className="font-medium mt-0.5">{formatTime(task.updatedAt, timezone)}</div>
                </div>
                <div className="rounded border border-border/70 bg-background/30 px-2 py-1.5">
                  <span className="text-muted-foreground">Started</span>
                  <div className="font-medium mt-0.5">{task.startedAt ? formatTime(task.startedAt, timezone) : '—'}</div>
                </div>
                <div className="rounded border border-border/70 bg-background/30 px-2 py-1.5">
                  <span className="text-muted-foreground">Subagent / Session</span>
                  <div className="font-mono mt-0.5 truncate" title={task.subagentId || ''}>{task.subagentId || '—'}</div>
                </div>
              </div>

              <div>
                <p className="text-[11px] text-muted-foreground mb-1">Status timeline</p>
                <div className="space-y-1.5 max-h-44 overflow-y-auto pr-1">
                  {runEntries.length === 0 && <p className="text-[11px] text-muted-foreground">No run events yet</p>}
                  {runEntries.map((run, idx) => {
                    const runStatus = run.status
                    const runDuration = formatDuration(run.startedAt, run.completedAt)
                    const runOutput = run.message || run.error || run.result
                    const copyLabel = `run-${run.id}`
                    return (
                      <div key={run.id} className="rounded border border-border/70 bg-background/30 px-2 py-1.5">
                        <div className="flex items-center justify-between gap-2">
                          <div className="flex items-center gap-2 min-w-0">
                            <span className={cn(
                              'text-[10px] px-1.5 py-0.5 rounded-full border capitalize',
                              runStatus === 'completed' && 'bg-green-500/10 text-green-400 border-green-500/30',
                              runStatus === 'failed' && 'bg-red-500/10 text-red-400 border-red-500/30',
                              runStatus === 'in-progress' && 'bg-amber-500/10 text-amber-400 border-amber-500/30',
                              runStatus === 'queued' && 'bg-zinc-500/10 text-zinc-400 border-zinc-500/30',
                              runStatus === 'canceled' && 'bg-orange-500/10 text-orange-400 border-orange-500/30',
                              runStatus === 'timeout' && 'bg-fuchsia-500/10 text-fuchsia-400 border-fuchsia-500/30',
                            )}>{runStatus}</span>
                            <span className="text-[11px] text-muted-foreground">Run {runEntries.length - idx}</span>
                            {runDuration && <span className="text-[10px] text-green-400">{runDuration}</span>}
                          </div>
                          {runOutput && (
                            <button
                              onClick={() => copyToClipboard(copyLabel, runOutput)}
                              className="text-[10px] px-1.5 py-0.5 rounded bg-secondary hover:bg-accent transition-colors inline-flex items-center gap-1"
                              title="Copy run output"
                            >
                              {copiedText === copyLabel ? <Check size={10} /> : <Copy size={10} />}
                              Copy
                            </button>
                          )}
                        </div>
                        <div className="mt-1 text-[10px] text-muted-foreground space-y-0.5">
                          <p>Started: {run.startedAt ? formatTime(run.startedAt, timezone) : '—'}</p>
                          <p>Updated: {run.updatedAt ? formatTime(run.updatedAt, timezone) : (run.completedAt ? formatTime(run.completedAt, timezone) : '—')}</p>
                          {run.reason && <p>Reason: {run.reason}</p>}
                          {run.message && <p className="line-clamp-2">Message: {run.message}</p>}
                          <p className="font-mono truncate" title={run.sessionId || run.subagentId || ''}>Session: {run.sessionId || run.subagentId || '—'}</p>
                        </div>
                      </div>
                    )
                  })}
                </div>
              </div>
            </div>

            {task.description && (
              <div>
                <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2">Description</h3>
                <p className="text-sm text-foreground/90">{task.description}</p>
              </div>
            )}

            {displayMessage && (
              <div ref={outputRef}>
                <MarkdownRenderer
                  content={displayMessage}
                  isError={hasError}
                  showToggle={true}
                  size="xs"
                  maxHeight="max-h-64"
                />
              </div>
            )}

            <AttachmentSection
              key={attKey}
              taskId={task.id}
              attachments={attachments}
              onChange={refreshAttachments}
              readOnly={false}
            />

            {filePaths.length > 0 && (
              <div>
                <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2">Linked Files</h3>
                <div className="space-y-1">
                  {filePaths.map(fp => (
                    <button
                      key={fp}
                      className="flex items-center gap-2 px-3 py-1.5 rounded-md bg-secondary/50 border border-border text-sm hover:bg-secondary hover:border-primary/30 transition-colors w-full text-left cursor-pointer"
                      onClick={() => { onClose(); navigate({ to: '/files', search: { openFile: fp } }) }}
                    >
                      <FileText size={13} className="text-muted-foreground shrink-0" />
                      <span className="font-mono text-xs truncate">{fp}</span>
                    </button>
                  ))}
                </div>
              </div>
            )}
          </div>

          <div className="w-full md:w-1/3 border-t md:border-t-0 md:border-l border-border overflow-y-auto">
            <div ref={historyRef} className="flex items-center gap-1.5 px-4 py-3 text-sm font-medium text-muted-foreground border-b border-border">
              <Activity size={14} />
              History
            </div>
            <ActivityLog taskId={task.id} />
          </div>
        </div>

        <div className="flex justify-end gap-2 px-5 py-4 border-t border-border shrink-0">
<button onClick={onClose} className="px-4 py-2 text-sm rounded-md bg-secondary hover:bg-accent transition-colors">Close</button>
        </div>
      </div>
    </div>
  )
}
