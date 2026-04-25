(function () {
  const ENDPOINT = 'https://hechizo-reporte-nuevo-production.up.railway.app/notify';
  const STORE_ID = (window.LS && window.LS.store && window.LS.store.id) ? window.LS.store.id : 0;

  // ── Estilos ──────────────────────────────────────────────────────────────────
  const style = document.createElement('style');
  style.textContent = `
    #sn-container { margin-top: 1rem; }
    #sn-btn {
      width: 100%; padding: 10px;
      border: 1px solid #6c757d; color: #6c757d;
      background: white; cursor: pointer;
      text-align: center; font-size: 14px;
    }
    #sn-btn:hover { background: #f8f9fa; }
    .sn-listing-btn {
      width: 100%; padding: 10px;
      border: 1px solid #6c757d; color: #6c757d;
      background: white; cursor: pointer;
      text-align: center; font-size: 14px;
      margin-top: 8px; display: block;
    }
    .sn-listing-btn:hover { background: #f8f9fa; }
    #sn-overlay {
      display: none; position: fixed;
      top: 0; left: 0; width: 100%; height: 100%;
      background: rgba(0,0,0,0.5); z-index: 9998;
    }
    #sn-modal {
      display: none; position: fixed;
      top: 50%; left: 50%;
      transform: translate(-50%, -50%);
      background: white; padding: 24px;
      z-index: 9999; width: 90%; max-width: 400px;
      border-radius: 4px;
    }
    #sn-close { float: right; cursor: pointer; font-size: 20px; color: #999; }
    #sn-title { font-size: 15px; margin: 16px 0 12px; clear: both; }
    #sn-product-name { font-weight: bold; font-size: 13px; margin-bottom: 12px; color: #444; }
    #sn-email {
      width: 100%; padding: 8px; border: 1px solid #ccc;
      margin-bottom: 12px; font-size: 14px; box-sizing: border-box;
    }
    #sn-submit {
      width: 100%; padding: 10px; background: #333;
      color: white; border: none; cursor: pointer; font-size: 14px;
    }
    #sn-submit:hover { background: #555; }
    #sn-success { display: none; text-align: center; padding: 16px 0; color: #555; }
    #sn-error { color: red; font-size: 12px; margin-bottom: 8px; display: none; }
    #sn-powered { text-align: center; font-size: 11px; color: #ccc; margin-top: 12px; }
  `;
  document.head.appendChild(style);

  // ── Modal compartido ─────────────────────────────────────────────────────────
  let _modal = null;
  let _currentProduct = null;

  function buildModal() {
    if (_modal) return;
    const div = document.createElement('div');
    div.innerHTML = `
      <div id="sn-overlay"></div>
      <div id="sn-modal">
        <span id="sn-close">×</span>
        <div id="sn-title">Dejanos tu email y te avisamos cuando el producto vuelva a estar disponible:</div>
        <div id="sn-product-name"></div>
        <div id="sn-error">Por favor ingresá un email válido.</div>
        <input id="sn-email" type="email" placeholder="tucorreo@email.com" />
        <button id="sn-submit">Avisame</button>
        <div id="sn-success">✅ ¡Te avisamos cuando vuelva!</div>
        <div id="sn-powered">Notificaciones de stock</div>
      </div>
    `;
    document.body.appendChild(div);
    _modal = true;

    document.getElementById('sn-overlay').addEventListener('click', closeModal);
    document.getElementById('sn-close').addEventListener('click', closeModal);
    document.getElementById('sn-submit').addEventListener('click', submitForm);
  }

  function openModal(productId, variantId, productName, variantName, sku) {
    buildModal();
    _currentProduct = { productId, variantId, productName, variantName, sku };

    document.getElementById('sn-product-name').textContent = productName;
    document.getElementById('sn-email').value = '';
    document.getElementById('sn-error').style.display = 'none';
    document.getElementById('sn-success').style.display = 'none';
    document.getElementById('sn-submit').style.display = '';
    document.getElementById('sn-submit').disabled = false;
    document.getElementById('sn-submit').textContent = 'Avisame';
    document.getElementById('sn-email').style.display = '';
    document.getElementById('sn-overlay').style.display = 'block';
    document.getElementById('sn-modal').style.display = 'block';
    document.getElementById('sn-email').focus();
  }

  function closeModal() {
    if (!_modal) return;
    document.getElementById('sn-overlay').style.display = 'none';
    document.getElementById('sn-modal').style.display = 'none';
  }

  async function submitForm() {
    const emailIn = document.getElementById('sn-email');
    const error   = document.getElementById('sn-error');
    const submit  = document.getElementById('sn-submit');
    const success = document.getElementById('sn-success');
    const email   = emailIn.value.trim();

    error.style.display = 'none';
    if (!email || !email.includes('@')) {
      error.textContent = 'Por favor ingresá un email válido.';
      error.style.display = 'block';
      return;
    }

    submit.disabled = true;
    submit.textContent = 'Enviando...';

    try {
      const { productId, variantId, productName, variantName, sku } = _currentProduct;
      const res = await fetch(ENDPOINT, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          email,
          product_id:   parseInt(productId),
          variant_id:   parseInt(variantId),
          store_id:     STORE_ID,
          product_name: productName,
          variant_name: variantName,
          sku:          sku || undefined,
        })
      });

      if (res.ok) {
        emailIn.style.display = 'none';
        submit.style.display = 'none';
        success.style.display = 'block';
      } else {
        error.textContent = 'Hubo un error, intentá de nuevo.';
        error.style.display = 'block';
        submit.disabled = false;
        submit.textContent = 'Avisame';
      }
    } catch (e) {
      error.textContent = 'Hubo un error, intentá de nuevo.';
      error.style.display = 'block';
      submit.disabled = false;
      submit.textContent = 'Avisame';
    }
  }

  // ── Helpers ──────────────────────────────────────────────────────────────────

  function extractProductIdFromUrl(url) {
    if (!url) return null;
    // TN product URLs: /products/slug o /productos/slug — el ID no está en la URL
    // Intentar data attributes del link
    return null;
  }

  function getSkuFromEl(el) {
    const skuEl = el ? el.querySelector('[data-sku], [itemprop="sku"], .product-sku, .js-sku') : null;
    return skuEl ? (skuEl.getAttribute('data-sku') || skuEl.getAttribute('content') || skuEl.textContent).trim() : '';
  }

  // ── Página de producto (detail) ───────────────────────────────────────────────

  function injectDetailPage() {
    const productId = (window.LS && window.LS.product && window.LS.product.id)
      ? String(window.LS.product.id) : null;
    if (!productId) return;

    const anchor = document.querySelector('form.js-product-form');
    if (!anchor) return;

    const nameEl = document.querySelector('h1.product-name, h1, .product-name');
    const productName = nameEl ? nameEl.textContent.trim() : '';

    // Crear el contenedor (oculto por defecto)
    const container = document.createElement('div');
    container.id = 'sn-container';
    container.style.display = 'none';
    container.innerHTML = `<button id="sn-btn">🔔 Avisame cuando haya stock</button>`;
    anchor.parentNode.insertBefore(container, anchor.nextSibling);

    document.getElementById('sn-btn').addEventListener('click', function() {
      const variantEl = document.querySelector('.js-variation-option.selected, .js-variation-option[data-selected], input[name="id"]');
      const variantId = variantEl
        ? (variantEl.getAttribute('data-id') || variantEl.getAttribute('data-variation-id') || variantEl.value || productId)
        : productId;
      const variantNameEl = document.querySelector('.js-variation-option.selected, .js-variation-option[data-selected]');
      const variantName = variantNameEl ? variantNameEl.textContent.trim() : '';
      const skuEl = document.querySelector('[data-sku], [itemprop="sku"], .product-sku, .js-sku');
      const sku = skuEl ? (skuEl.getAttribute('data-sku') || skuEl.getAttribute('content') || skuEl.textContent).trim() : '';
      openModal(productId, variantId, productName, variantName, sku);
    });

    // Observar el botón de compra — cuando se vuelve disabled = variante sin stock
    const addToCartBtn = document.querySelector('.js-addtocart');
    if (!addToCartBtn) return;

    function updateVisibility() {
      const noStock = addToCartBtn.classList.contains('nostock') ||
                      addToCartBtn.classList.contains('disabled') ||
                      addToCartBtn.disabled;
      container.style.display = noStock ? '' : 'none';
    }

    // Estado inicial
    updateVisibility();

    // Observar cambios de clase/atributo en el botón de compra
    new MutationObserver(updateVisibility).observe(addToCartBtn, {
      attributes: true,
      attributeFilter: ['class', 'disabled'],
    });
  }

  // ── Páginas de listado ────────────────────────────────────────────────────────

  function _parseVariants(card) {
    // TN themes put variant JSON in data-variants on .js-quickshop-container or .js-product-container
    const containers = card.querySelectorAll('[data-variants]');
    for (const el of containers) {
      try {
        const raw = el.getAttribute('data-variants');
        if (!raw) continue;
        const parsed = JSON.parse(raw);
        // data-variants can be an object (single variant) or array
        return Array.isArray(parsed) ? parsed : [parsed];
      } catch (e) { /* ignore */ }
    }
    return null;
  }

  function injectListingPage() {
    const cards = document.querySelectorAll('.js-item-product[data-product-id], [data-product-id].item-product');

    cards.forEach(function(card) {
      if (card.dataset.snInjected) return;

      const productId = card.dataset.productId;
      if (!productId) return;

      const variants = _parseVariants(card);
      let variantId = productId;

      if (variants && variants.length > 0) {
        // Only show if ALL variants are out of stock
        const allOutOfStock = variants.every(function(v) {
          return v.available === false || v.stock === 0;
        });
        if (!allOutOfStock) return;
        variantId = String(variants[0].id || productId);
      } else {
        // Fallback: check for disabled/nostock addtocart button
        const btn = card.querySelector('.js-addtocart.nostock, .js-addtocart.disabled, .js-addtocart[disabled]');
        if (!btn) return;
        variantId =
          btn.dataset.variantId ||
          btn.dataset.itemId ||
          card.querySelector('input[name="id"]')?.value ||
          productId;
      }

      card.dataset.snInjected = '1';

      const productName =
        card.querySelector('.js-item-name, [itemprop="name"], h2, h3, .item-name, .product-name')?.textContent.trim() || '';

      const sku = getSkuFromEl(card);

      const notifyBtn = document.createElement('button');
      notifyBtn.className = 'sn-listing-btn';
      notifyBtn.textContent = '🔔 Avisame cuando haya stock';

      notifyBtn.addEventListener('click', function(e) {
        e.preventDefault();
        e.stopPropagation();
        openModal(productId, variantId, productName, '', sku);
      });

      // Inject into NubeSDK slot if available (same approach as other TN apps)
      const slot = card.querySelector('.js-nubesdk-slot[data-nubesdk-slot="after_product_grid_item_name"]');
      if (slot) {
        slot.appendChild(notifyBtn);
        return;
      }
      // Fallback: after product name element
      const nameEl = card.querySelector('.js-item-name, [itemprop="name"], .item-name, .product-name');
      if (nameEl && nameEl.parentNode) {
        nameEl.parentNode.insertBefore(notifyBtn, nameEl.nextSibling);
      } else {
        card.appendChild(notifyBtn);
      }
    });
  }

  // ── Init ──────────────────────────────────────────────────────────────────────

  function init() {
    // Si es página de producto individual (window.LS.product existe)
    if (window.LS && window.LS.product && window.LS.product.id) {
      injectDetailPage();
    } else {
      // Página de listado / colección
      injectListingPage();
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
