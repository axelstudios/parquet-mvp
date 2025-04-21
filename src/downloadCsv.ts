import { unparse } from 'papaparse'

export const convertToCsv = (data: Record<string, any>[], fields: string[]) => unparse({ fields, data })

export const downloadCsv = (csv: string, name: string) => {
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })

  const url = URL.createObjectURL(blob)

  const link = document.createElement('a')
  link.href = url
  link.setAttribute('download', name)

  document.body.appendChild(link)

  link.click()

  document.body.removeChild(link)
  URL.revokeObjectURL(url)
}
