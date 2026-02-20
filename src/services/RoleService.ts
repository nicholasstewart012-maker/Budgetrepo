export class RoleService {
    /**
     * Evaluates if a given email is present in a semicolon-delimited list of emails.
     */
    public static isUserInRole(userEmail: string, approverEmailsString: string): boolean {
        if (!userEmail || !approverEmailsString) {
            return false;
        }

        const emailList = approverEmailsString
            .split(';')
            .map(email => email.trim().toLowerCase())
            .filter(email => email.length > 0);

        return emailList.indexOf(userEmail.toLowerCase()) > -1;
    }
}
