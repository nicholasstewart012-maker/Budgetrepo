import { WebPartContext } from '@microsoft/sp-webpart-base';
import { MSGraphClientV3 } from '@microsoft/sp-http';
import { IUser } from '../models/IUser';

export class GraphService {
    private context: WebPartContext;
    private isLocal: boolean;
    private managerCache: IUser | null = null;
    private directReportsCache: IUser[] | null = null;

    constructor(context: WebPartContext, isLocal: boolean = false) {
        this.context = context;
        this.isLocal = isLocal;
    }

    private async getClient(): Promise<MSGraphClientV3 | null> {
        if (this.isLocal) return null;
        return await this.context.msGraphClientFactory.getClient('3');
    }

    public async getMyManager(): Promise<IUser | null> {
        if (this.isLocal) {
            return { displayName: 'Local Manager', email: 'manager@contoso.local' };
        }

        if (this.managerCache) {
            return this.managerCache;
        }

        try {
            const client = await this.getClient();
            if (!client) return null;

            const response = await client.api('/me/manager').select('displayName,mail,id,userPrincipalName').get();
            if (response) {
                this.managerCache = {
                    displayName: response.displayName,
                    email: response.mail || response.userPrincipalName,
                    id: response.id
                };
                return this.managerCache;
            }
        } catch (error) {
            console.warn('Error retrieving manager:', error);
            // Fallback if no manager found or graph api fails
            return null;
        }
        return null;
    }

    public async getMyDirectReports(): Promise<IUser[]> {
        if (this.isLocal) {
            return [{ displayName: 'Local Report', email: 'report@contoso.local' }];
        }

        const cache = this.directReportsCache;
        if (cache) {
            return cache;
        }

        try {
            const client = await this.getClient();
            if (!client) return [];

            const response = await client.api('/me/directReports').select('displayName,mail,id,userPrincipalName').get();
            if (response && response.value) {
                this.directReportsCache = response.value.map((user: any) => ({
                    displayName: user.displayName,
                    email: user.mail || user.userPrincipalName,
                    id: user.id
                }));
                return this.directReportsCache || [];
            }
        } catch (error) {
            console.warn('Error retrieving direct reports:', error);
            return [];
        }
        return [];
    }
}
