import { useEffect, useRef, useCallback } from 'react'
import type { SchematicLine } from '../types'

export type FlowMotionLayerProps = {
  lines: SchematicLine[]
  width: number
  height: number
  padX: number
  padY: number
}

type Particle = {
  lineIdx: number
  t: number
  speed: number
}

function parsePath(d: string): { x: number; y: number }[] {
  const points: { x: number; y: number }[] = []
  const parts = d.match(/[ML]\s*[\d.e+-]+\s+[\d.e+-]+/gi)
  if (!parts) return points
  for (const part of parts) {
    const nums = part.match(/[\d.e+-]+/g)
    if (nums && nums.length >= 2) {
      points.push({ x: parseFloat(nums[0]), y: parseFloat(nums[1]) })
    }
  }
  return points
}

function interpolatePath(points: { x: number; y: number }[], t: number): { x: number; y: number } | null {
  if (points.length < 2) return null
  const totalSegs = points.length - 1
  const segF = t * totalSegs
  const seg = Math.min(Math.floor(segF), totalSegs - 1)
  const localT = segF - seg
  const a = points[seg]
  const b = points[seg + 1]
  return { x: a.x + (b.x - a.x) * localT, y: a.y + (b.y - a.y) * localT }
}

const MAX_PARTICLES = 3000
const BASE_SPEED = 0.003

function FlowMotionLayer({ lines, width, height, padX, padY }: FlowMotionLayerProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const particlesRef = useRef<Particle[]>([])
  const pathsRef = useRef<{ x: number; y: number }[][]>([])
  const rafRef = useRef<number>(0)

  useEffect(() => {
    pathsRef.current = lines.map((l) => parsePath(l.path))

    const particles: Particle[] = []
    const totalFlow = lines.reduce((s, l) => s + (l.flow ?? 1), 0)

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i]
      const flow = line.flow ?? 1
      const count = Math.max(1, Math.round((flow / Math.max(totalFlow, 1)) * MAX_PARTICLES))
      const speed = BASE_SPEED * Math.sqrt(Math.max(flow, 0.1)) * 0.01
      for (let j = 0; j < count; j++) {
        particles.push({ lineIdx: i, t: Math.random(), speed: Math.max(speed, 0.001) })
      }
    }
    particlesRef.current = particles
  }, [lines])

  const resizeCanvas = useCallback(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const parent = canvas.parentElement
    if (!parent) return
    const dpr = window.devicePixelRatio || 1
    const domW = parent.clientWidth
    const domH = parent.clientHeight
    canvas.width = domW * dpr
    canvas.height = domH * dpr
    canvas.style.width = `${domW}px`
    canvas.style.height = `${domH}px`
  }, [])

  useEffect(() => {
    resizeCanvas()
    window.addEventListener('resize', resizeCanvas)
    return () => window.removeEventListener('resize', resizeCanvas)
  }, [resizeCanvas])

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    let running = true

    const animate = () => {
      if (!running) return
      const domW = canvas.clientWidth
      const domH = canvas.clientHeight
      const dpr = window.devicePixelRatio || 1

      ctx.setTransform(1, 0, 0, 1, 0, 0)
      ctx.clearRect(0, 0, canvas.width, canvas.height)

      // Scale from viewBox coords to canvas pixels
      const scaleX = (domW * dpr) / width
      const scaleY = (domH * dpr) / height
      ctx.scale(scaleX, scaleY)

      const particles = particlesRef.current
      const paths = pathsRef.current

      ctx.fillStyle = 'rgba(255,255,255,0.85)'
      for (const p of particles) {
        p.t += p.speed
        if (p.t > 1) p.t -= 1

        const pts = paths[p.lineIdx]
        if (!pts || pts.length < 2) continue
        const pos = interpolatePath(pts, p.t)
        if (!pos) continue

        ctx.beginPath()
        ctx.arc(pos.x + padX, pos.y + padY, 1.2, 0, Math.PI * 2)
        ctx.fill()
      }

      rafRef.current = requestAnimationFrame(animate)
    }

    rafRef.current = requestAnimationFrame(animate)
    return () => {
      running = false
      cancelAnimationFrame(rafRef.current)
    }
  }, [lines, width, height, padX, padY])

  return (
    <canvas
      ref={canvasRef}
      style={{
        position: 'absolute',
        top: 0,
        left: 0,
        pointerEvents: 'none',
      }}
    />
  )
}

export default FlowMotionLayer
