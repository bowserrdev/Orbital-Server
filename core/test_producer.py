import asyncio
from database import queue_db
from loguru import logger

# Sovrascriviamo la funzione worker per simulare il crash
async def crashing_worker(job):
    logger.info(f"💀 Worker ha preso {job['imdb_id']}... e ora MUORE!")
    # Simuliamo crash brutale (senza blocco finally che pulisce)
    # In un processo reale equivale a un Kill -9 o un OOM (Out Of Memory)
    raise SystemExit("Crash simulato!")

async def scenario_worker_crash():
    await queue_db.connect()
    
    # 1. Pulizia e Inserimento
    async with queue_db.pool.acquire() as conn:
        await conn.execute("TRUNCATE jobs_active, job_subscriptions, scheduler_state CASCADE")
    
    logger.info("inserisco job destinato a morire...")
    res = await queue_db.enqueue_job("tt_ZOMBIE", "movie", 0, "user_A", "ch_A")
    job_id = res['job_id']

    # 2. Simuliamo l'estrazione (come fa lo Scheduler)
    async with queue_db.pool.acquire() as conn:
        async with conn.transaction():
            # Passiamo batch_size=1 esplicitamente
            jobs = await queue_db.fetch_and_claim_next_jobs(conn, batch_size=1)
    
    if jobs:
        job = jobs[0] # <--- PRENDIAMO IL PRIMO ELEMENTO
        
        # 3. Il Worker prende il job e muore
        try:
            await crashing_worker(job)
        except SystemExit:
            logger.warning("Il worker è crashato come previsto.")

    # 4. ANALISI DEI DANNI
    # Il job dovrebbe essere sparito da jobs_active (perché fetch_and_claim fa DELETE)
    # Ma la subscription?
    
    active = await queue_db.pool.fetchval("SELECT count(*) FROM jobs_active")
    subs = await queue_db.pool.fetchval("SELECT count(*) FROM job_subscriptions")
    
    logger.info(f"📊 Stato Post-Crash:")
    logger.info(f"   - Jobs Active: {active} (Atteso: 0)")
    logger.info(f"   - Subscriptions: {subs} (Atteso: 1 -> ZOMBIE!)")
    
    if active == 0 and subs > 0:
        logger.error("❌ RILEVATO PROBLEMA: Abbiamo una subscription orfana! L'utente aspetterà in eterno.")
    else:
        logger.success("✅ Miracolo (o errore nel test): Nessun orfano.")

    await queue_db.close()

if __name__ == "__main__":
    asyncio.run(scenario_worker_crash())