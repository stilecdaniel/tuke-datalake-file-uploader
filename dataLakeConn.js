const { BlobServiceClient } = require('@azure/storage-blob');
require('dotenv').config();

// Initialize the BlobServiceClient
const blobServiceClient = BlobServiceClient.fromConnectionString(process.env.AZURE_CONNECTION_STRING);

async function pushFile(file, channel, type) {
    const containerName = channel.toLowerCase().replace(/[^a-z0-9]/g, ''); // Azure container names must be lowercase

    try {
        // Get container client
        const containerClient = blobServiceClient.getContainerClient(containerName);

        // Create container if it doesn't exist
        await containerClient.createIfNotExists();

        // Generate object name based on type
        let blobName;
        const date = new Date().toISOString().slice(0, 10);
        const timestamp = Date.now();

        switch (type) {
            case 'viewership':
                blobName = `${channel}/viewership/${date}/${timestamp}.json`;
                break;
            case 'program':
                blobName = `${channel}/program/${date}/${timestamp}.json`;
                break;
            default:
                blobName = `${channel}/other/${date}/${timestamp}.json`;
        }

        // Get blob client
        const blockBlobClient = containerClient.getBlockBlobClient(blobName);

        // Upload the file
        const buffer = Buffer.from(file);
        const uploadBlobResponse = await blockBlobClient.upload(buffer, buffer.length, {
            blobHTTPHeaders: {
                blobContentType: 'application/json',
                blobCacheControl: 'no-cache'
            }
        });

        console.log(`Upload successful: ${blobName}`);

    } catch (err) {
        console.error('Error uploading to Azure Blob:', err);
    }
}

module.exports = pushFile;