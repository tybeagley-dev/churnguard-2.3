import fs from 'fs/promises';
import path from 'path';

class HubSpotIdTranslator {
  constructor() {
    this.translationMap = new Map();
    this.initialized = false;
  }

  async initializeTranslationMap() {
    if (this.initialized) return;

    try {
      const translationFilePath = path.join(process.cwd(), 'assets', 'churnguard_translation_table_corrected.json');
      const translationData = await fs.readFile(translationFilePath, 'utf8');
      const translationTable = JSON.parse(translationData);

      for (const entry of translationTable.translations) {
        this.translationMap.set(entry.account_id, entry.correct_hubspot_id);
      }

      console.log(`HubSpot ID Translator initialized with ${this.translationMap.size} translations`);
      this.initialized = true;

    } catch (error) {
      console.error('Failed to initialize HubSpot ID Translator:', error);
      throw new Error('Could not load HubSpot ID translation table');
    }
  }

  async getCorrectHubSpotId(accountId, originalHubspotId = '') {
    await this.initializeTranslationMap();

    const correctedId = this.translationMap.get(accountId);

    if (correctedId) {
      console.log(`HubSpot ID translation applied for ${accountId}: ${originalHubspotId} -> ${correctedId}`);
      return correctedId;
    }

    return originalHubspotId;
  }

  async hasTranslation(accountId) {
    await this.initializeTranslationMap();
    return this.translationMap.has(accountId);
  }

  async getAllTranslations() {
    await this.initializeTranslationMap();
    return new Map(this.translationMap);
  }

  async getTranslationStats() {
    await this.initializeTranslationMap();

    return {
      totalTranslations: this.translationMap.size,
      translatedAccountIds: Array.from(this.translationMap.keys())
    };
  }
}

export const hubspotIdTranslator = new HubSpotIdTranslator();