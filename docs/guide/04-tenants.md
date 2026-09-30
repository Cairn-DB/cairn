# 4. Customers and tenants

A tenant is a customer, a user, or any unit whose data must stay apart. Cairn enforces the
separation on the server: a request that acts for a tenant only ever reads, searches, updates
and deletes that tenant's documents, whatever filter or id it sends.

## Two ways to act for a tenant

**One backend for every customer** (most SaaS backends): an unscoped key, and a view per
customer.

```python
def for_customer(customer_id: str) -> Client:
    return articles.with_tenant(customer_id)   # every call stays inside this customer
```

The view sends the `Cairn-Tenant` header. Your backend decides which customer a request is
for (from its own authentication); Cairn guarantees that nothing leaks across.

**A key per customer**, when untrusted code talks to Cairn directly (a customer-side agent, a
plugin):

```bash
docker exec cairn cairn-server keygen acme-agent read,write --tenant acme
```

That key reaches `acme` only. A request with it that names another tenant gets 403. A scoped
key cannot hold the `admin` role.

## What changes inside a tenant

- **Ids belong to the tenant.** `acme`'s `"faq"` and `globex`'s `"faq"` are two documents.
- **Reads, searches and deletions** only reach the tenant's documents: the server adds the
  restriction itself. A search with no filter returns only that tenant's documents.
- Tenant names have 1 to 128 letters, digits, `_`, `.` or `-`. Use your customer's stable id.
- Without a tenant, an unscoped key sees every tenant's documents, marked with `_tenant`
  (useful for admin tools, not for request handlers).

## Erasing a customer

```python
result = articles.forget_tenant("globex")               # every article chunk of globex
cairn.collection("tickets").forget_tenant("globex")
```

One call per collection removes every document of the customer, on every replica, and is
audited. It needs an unscoped key with the `takedown` role. Chapter 6 shows how to prove it.

Next: [document lifecycle](05-lifecycle.md).
