// Clear the session cookie and return to the landing page.
export default function handler(req, res) {
  res.setHeader(
    'Set-Cookie',
    'sf_session=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0',
  );
  res.writeHead(302, { Location: '/' });
  res.end();
}
