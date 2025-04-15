import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3'

// Constants
const bucket = 'oedi-data-lake'
const s3Client = new S3Client({
  region: 'us-west-2',
  credentials: {accessKeyId: '', secretAccessKey: ''},
  signer: {sign: async req => req},
})

export async function getS3File(key: string): Promise<ArrayBuffer | undefined> {
  const command = new GetObjectCommand({
    Bucket: bucket,
    Key: key,
  })

  try {
    const response = await s3Client.send(command)

    // @ts-ignore
    return new Response(response.Body).arrayBuffer()
  } catch (err) {
    console.error('Error fetching the file from S3', err)
    return
  }
}
