export default function CompareLoading() {
  return (
    <main className="container-fluid py-4" aria-busy="true">
      <h1>Compare</h1>
      <p role="status">Loading comparison…</p>
      <p>
        The two sides are being prepared. You can leave this page while they
        load.
      </p>
    </main>
  );
}
