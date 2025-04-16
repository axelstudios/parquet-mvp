import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { getTimestamp } from './utils'

// Constants
const bucket = 'bstkanalysis.bps-workflow'
const s3Client = new S3Client({
  region: 'us-west-2',
  credentials: {accessKeyId: '', secretAccessKey: ''},
  signer: {sign: async req => req},
})

export async function uploadS3File(csv: string) {
  const command = new PutObjectCommand({
    Bucket: bucket,
    Key: `user_input_comstock_gui/${getTimestamp()}_user_queries.csv`,
    Body: csv,
    ContentType: 'text/csv',
  })

  try {
    await s3Client.send(command)
  } catch (err) {
    console.error('Error uploading the file to S3', err)
  }
}
